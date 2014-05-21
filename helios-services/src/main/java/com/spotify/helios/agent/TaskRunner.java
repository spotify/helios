package com.spotify.helios.agent;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.agent.docker.ContainerNotFoundException;
import com.spotify.helios.agent.docker.DockerClient;
import com.spotify.helios.agent.docker.DockerException;
import com.spotify.helios.agent.docker.DockerTimeoutException;
import com.spotify.helios.agent.docker.ImageNotFoundException;
import com.spotify.helios.agent.docker.ImagePullFailedException;
import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.ContainerCreation;
import com.spotify.helios.agent.docker.messages.ContainerExit;
import com.spotify.helios.agent.docker.messages.ContainerInfo;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import com.spotify.helios.serviceregistration.ServiceRegistrationHandle;
import com.spotify.helios.servicescommon.InterruptingExecutionThreadService;
import com.spotify.helios.servicescommon.statistics.MetricsContext;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A runner service that starts a container once.
 */
class TaskRunner extends InterruptingExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

  private static final long CONTAINER_START_TIMEOUT_SECONDS = 60;

  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newCachedThreadPool(), 0, SECONDS));

  private final long delayMillis;
  private final SettableFuture<Integer> resultFuture = SettableFuture.create();
  private final ServiceRegistrar registrar;
  private final CommandWrapper commandWrapper;
  private final Job job;
  private final ContainerUtil containerUtil;
  private final SupervisorMetrics metrics;
  private final DockerClient docker;
  private final FlapController flapController;
  private final AtomicReference<ThrottleState> throttle;
  private final StatusUpdater statusUpdater;
  private final Supplier<String> containerIdSupplier;

  private ListenableFuture<Void> startFuture;

  public TaskRunner(final long delayMillis,
                    final ServiceRegistrar registrar,
                    final Job job,
                    final CommandWrapper commandWrapper,
                    final ContainerUtil containerUtil,
                    final SupervisorMetrics metrics,
                    final DockerClient docker,
                    final FlapController flapController,
                    final AtomicReference<ThrottleState> throttle,
                    final StatusUpdater statusUpdater,
                    final Supplier<String> containerIdSupplier) {
    super("TaskRunner(" + job.toString() + ")");
    this.delayMillis = delayMillis;
    this.registrar = registrar;
    this.job = job;
    this.commandWrapper = commandWrapper;
    this.containerUtil = containerUtil;
    this.metrics = metrics;
    this.docker = docker;
    this.flapController = flapController;
    this.throttle = throttle;
    this.statusUpdater = statusUpdater;
    this.containerIdSupplier = containerIdSupplier;
  }

  @SuppressWarnings("TryWithIdenticalCatches")
  @Override
  public void run() {
    try {
      metrics.supervisorRun();
      // Delay
      Thread.sleep(delayMillis);

      // Get persisted status
      final String registeredContainerId = containerIdSupplier.get();

      // Find out if the container is already running
      final ContainerInfo containerInfo =
          getRunningContainerInfo(registeredContainerId);

      // Create and start container if necessary
      final String containerId = maybeCreateAndStartContainer(registeredContainerId, containerInfo);

      // Expose ports
      final ContainerInfo runningContainerInfo = docker.inspectContainer(containerId);

      // TODO (dano): Now that we perform the port allocation in Helios instead of relying on
      // docker, report the ports we allocated instead of parsing the docker output.
      final Map<String, PortMapping> ports = containerUtil.parsePortBindings(runningContainerInfo);
      statusUpdater.setStatus(RUNNING, containerId, ports);
      metrics.containersRunning();

      // Wait for container to die
      flapController.jobStarted();
      final ServiceRegistrationHandle registrationHandle = serviceRegister(ports);
      final int exitCode;
      try {
        exitCode = flapController.waitFuture(waitContainer(containerId)).statusCode();
      } finally {
        serviceDeregister(registrationHandle);
      }
      log.info("container exited: {}: {}: {}", job, containerId, exitCode);
      flapController.jobDied();
      throttle.set(flapController.isFlapping() ? ThrottleState.FLAPPING : ThrottleState.NO);
      statusUpdater.setStatus(EXITED, containerId, ports);
      metrics.containersExited();
      resultFuture.set(exitCode);
    } catch (DockerTimeoutException e) {
      metrics.dockerTimeout();
      resultFuture.setException(e);
    } catch (InterruptedException e) {
      resultFuture.setException(e);
    } catch (Throwable e) {
      // Keep separate catch clauses to simplify setting breakpoints on actual errors
      metrics.containersThrewException();
      resultFuture.setException(e);
    } finally {
      if (!resultFuture.isDone()) {
        log.error("result future not set!");
        resultFuture.setException(new Exception("result future not set!"));
      }
    }
  }

  private ListenableFuture<ContainerExit> waitContainer(final String containerId) {
    return executorService.submit(new Callable<ContainerExit>() {
      @Override
      public ContainerExit call() throws Exception {
        return docker.waitContainer(containerId);
      }
    });
  }

  private ListenableFuture<Void> startContainer(final String containerId,
                                                final HostConfig hostConfig) {
    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        docker.startContainer(containerId, hostConfig);
        return null;
      }
    });
  }

  private String maybeCreateAndStartContainer(final String registeredContainerId,
                                              final ContainerInfo containerInfo)
      throws DockerException, InterruptedException {
    if (containerInfo != null && containerInfo.state().running()) {
      return registeredContainerId;
    }

    // Ensure we have the image
    final String image = job.getImage();
    try {
      statusUpdater.setStatus(PULLING_IMAGE);
      maybePullImage(image);
    } catch (ImagePullFailedException e) {
      throttle.set(ThrottleState.IMAGE_PULL_FAILED);
      statusUpdater.setStatus(FAILED);
      throw e;
    } catch (ImageNotFoundException e) {
      throttle.set(ThrottleState.IMAGE_MISSING);
      statusUpdater.setStatus(FAILED);
      throw e;
    }
    return startContainer(image);
  }

  public ListenableFuture<Integer> result() {
    return resultFuture;
  }

  private ServiceRegistrationHandle serviceRegister(Map<String, PortMapping> ports)
      throws InterruptedException {
    if (registrar == null) {
      return null;
    }

    final ServiceRegistration.Builder builder = ServiceRegistration.newBuilder();

    for (final Entry<ServiceEndpoint, ServicePorts> entry :
        job.getRegistration().entrySet()) {
      final ServiceEndpoint registration = entry.getKey();
      final ServicePorts servicePorts = entry.getValue();
      for (String portName : servicePorts.getPorts().keySet()) {
        final PortMapping mapping = ports.get(portName);
        if (mapping == null) {
          log.error("no '{}' port mapped for registration: '{}'", portName, registration);
          continue;
        }
        Integer externalPort = mapping.getExternalPort();
        if (externalPort == null) {
          log.error("no external '{}' port for registration: '{}'", portName, registration);
          continue;
        }
        builder.endpoint(registration.getName(), registration.getProtocol(), externalPort);
      }
    }

    return registrar.register(builder.build());
  }

  private void serviceDeregister(final ServiceRegistrationHandle handle) {
    if (registrar == null) {
      return;
    }

    registrar.unregister(handle);
  }

  private String startContainer(final String image)
      throws InterruptedException, DockerException {
    statusUpdater.setStatus(CREATING, null);
    final ContainerConfig containerConfig = containerUtil.containerConfig(job);

    final ImageInfo imageInfo = docker.inspectImage(image);
    if (imageInfo == null) {
      throw new HeliosRuntimeException("docker inspect image returned null on image " + image);
    }
    commandWrapper.modifyCreateConfig(image, job, imageInfo, containerConfig);

    final String name = ContainerUtil.containerName(job.getId());
    final ContainerCreation container;
    container = docker.createContainer(containerConfig, name);

    final String containerId = container.id();
    log.info("created container: {}: {}, {}", job, container, containerConfig);

    final HostConfig hostConfig = containerUtil.hostConfig();
    commandWrapper.modifyStartConfig(hostConfig);

    statusUpdater.setStatus(STARTING, containerId);
    log.info("starting container: {}: {} {}", job, containerId, hostConfig);

    startFuture = startContainer(containerId, hostConfig);
    try {
      startFuture.get(CONTAINER_START_TIMEOUT_SECONDS, SECONDS);
    } catch (ExecutionException | TimeoutException e) {
      throw Throwables.propagate(e);
    }

    log.info("started container: {}: {}", job, containerId);
    metrics.containerStarted();
    return containerId;
  }

  private ContainerInfo getRunningContainerInfo(final String registeredContainerId)
      throws DockerException, InterruptedException {
    if (registeredContainerId == null) {
      return null;
    }
    log.info("inspecting container: {}: {}", job, registeredContainerId);
    try {
      return docker.inspectContainer(registeredContainerId);
    } catch (ContainerNotFoundException e) {
      return null;
    }
  }

  private void maybePullImage(final String image) throws DockerException, InterruptedException {
    try {
      docker.inspectImage(image);
      metrics.imageCacheHit();
      return;
    } catch (ImageNotFoundException ignore) {
      metrics.imageCacheMiss();
    }
    final MetricsContext metric = metrics.containerPull();
    try {
      docker.pull(image);
      metric.success();
    } catch (DockerException e) {
      metric.failure();
      throw e;
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Wait for eventual outstanding start request to finish
    final ListenableFuture<Void> future = startFuture;
    if (future != null) {
      try {
        future.get();
      } catch (ExecutionException | CancellationException e) {
        log.debug("exception from docker start request", e);
      }
    }
  }
}