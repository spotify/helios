/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.PortBinding;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.InterruptingExecutionThreadService;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.MetricsContext;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;
import com.spotify.nameless.client.NamelessRegistrar;
import com.spotify.nameless.client.RegistrationHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Supervises docker containers for a single job.
 */
class Supervisor {

  public static final int HOST_NAME_MAX = 64;

  public interface Listener {
    void stateChanged(Supervisor supervisor);
  }

  private static class PullingException extends Exception {}

  private static final Logger log = LoggerFactory.getLogger(Supervisor.class);

  public static final ThreadFactory RUNNER_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-supervisor-runner-%d").build();

  private static final int DOCKER_REQUEST_TIMEOUT_SECONDS = 5;
  private static final int DOCKER_LONG_REQUEST_TIMEOUT_SECONDS = 30;
  private static final long PULL_POLL_TIMEOUT_SECONDS = 30;

  private final MonitoredDockerClient docker;
  private final JobId jobId;
  private final Job job;
  private final AgentModel model;
  private final Map<String, String> envVars;
  private final FlapController flapController;
  private final RestartPolicy restartPolicy;
  private final TaskStatusManager stateManager;
  private final NamelessRegistrar registrar;
  private final CommandWrapper commandWrapper;
  private final String host;
  private final SupervisorMetrics metrics;
  private final Reactor reactor;
  private final Map<String, Integer> ports;
  private final Listener listener;
  private final ListeningExecutorService pullStreamExecutor;

  private volatile Runner runner;
  private volatile Command currentCommand;
  private volatile Command performedCommand;
  private volatile ThrottleState throttle = ThrottleState.NO;

  public Supervisor(final Builder builder) {
    this.ports = builder.ports;
    this.jobId = checkNotNull(builder.jobId);
    this.job = checkNotNull(builder.job);
    this.model = checkNotNull(builder.model);
    this.docker = new MonitoredDockerClient(checkNotNull(builder.dockerClient),
        builder.metrics, builder.riemannFacade, DOCKER_REQUEST_TIMEOUT_SECONDS,
        DOCKER_LONG_REQUEST_TIMEOUT_SECONDS, 120);
    this.restartPolicy = checkNotNull(builder.restartPolicy);
    this.envVars = checkNotNull(builder.envVars);
    this.flapController = checkNotNull(builder.flapController);
    this.stateManager = checkNotNull(builder.stateManager);
    this.registrar = builder.registrar;
    this.commandWrapper = checkNotNull(builder.commandWrapper);
    this.host = checkNotNull(builder.host);
    this.metrics = checkNotNull(builder.metrics);
    this.listener = builder.listener;
    this.currentCommand = new Nop();
    this.reactor = new DefaultReactor("supervisor-" + jobId, new Update(), SECONDS.toMillis(30));
    this.reactor.startAsync();
    this.pullStreamExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
  }

  /**
   * Start the job.
   */
  public void start() {
    currentCommand = new Start();
    reactor.signal();
    metrics.supervisorStarted();
  }

  /**
   * Stop the job.
   */
  public void stop() throws InterruptedException {
    currentCommand = new Stop();
    reactor.signal();
    metrics.supervisorStopped();
  }

  /**
   * Close this job. The actual container is left as-is.
   */
  public void close() {
    reactor.stopAsync();
    if (runner != null) {
      runner.stopAsync();
    }
    metrics.supervisorClosed();
  }

  /**
   * Wait for supervisor to stop after closing it.
   */
  public void join() {
    reactor.awaitTerminated();
    if (runner != null) {
      runner.awaitTerminated();
    }
  }

  /**
   * Check if the current command is start.
   */
  public boolean isStarting() {
    return currentCommand instanceof Start;
  }

  /**
   * Check if the current command is stop.
   */
  public boolean isStopping() {
    return currentCommand instanceof Stop;
  }

  /**
   * Check whether the last start/stop command is done.
   */
  public boolean isDone() {
    return currentCommand == performedCommand;
  }

  /**
   * Persist job status.
   */
  private void setStatus(final TaskStatus.State status, final String containerId) {
    setStatus(status, containerId, null);
  }

  /**
   * Persist job status with port mapping.
   */
  private void setStatus(final TaskStatus.State status, final String containerId,
                         final Map<String, PortMapping> ports) {
    stateManager.setStatus(status, throttle, containerId, ports, getContainerEnvMap(job));
  }

  /**
   * Get the current job status.
   */
  public TaskStatus.State getStatus() {
    return stateManager.getStatus();
  }

  /**
   * Create docker container configuration for a job.
   */
  private ContainerConfig containerConfig(final Job descriptor) {
    final ContainerConfig containerConfig = new ContainerConfig();
    containerConfig.setImage(descriptor.getImage());
    final List<String> command = descriptor.getCommand();
    containerConfig.setCmd(command.toArray(new String[command.size()]));
    containerConfig.setEnv(containerEnv(descriptor));
    containerConfig.setExposedPorts(containerExposedPorts(descriptor));
    containerConfig.setHostName(safeHostNameify(descriptor.getId().getName() + "_" +
                                                descriptor.getId().getVersion()));
    containerConfig.setDomainName(host);
    return containerConfig;
  }

  private String safeHostNameify(String name) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if ( (c >= 'A' && c <= 'Z')
           || (c >= 'a' && c <= 'z')
           || (c >= '0' && c <= '9')) {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    return truncate(sb.toString(), HOST_NAME_MAX);
  }

  private static String truncate(final String s, final int len) {
    return s.substring(0, Math.min(len, s.length()));
  }

  /**
   * Create container port exposure configuration for a job.
   */
  private Map<String, Void> containerExposedPorts(final Job job) {
    final Map<String, Void> ports = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> entry : job.getPorts().entrySet()) {
      final PortMapping mapping = entry.getValue();
      ports.put(containerPort(mapping.getInternalPort(), mapping.getProtocol()), null);
    }
    return ports;
  }

  /**
   * Create a docker port exposure/mapping entry.
   */
  private String containerPort(final int port, final String protocol) {
    return port + "/" + protocol;
  }

  /**
   * Create a container host configuration for the job.
   */
  private HostConfig hostConfig() {
    final HostConfig hostConfig = new HostConfig();
    hostConfig.portBindings = portBindings();
    return hostConfig;
  }

  /**
   * Create a port binding configuration for the job.
   */
  private Map<String, List<PortBinding>> portBindings() {
    final Map<String, List<PortBinding>> bindings = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> e : job.getPorts().entrySet()) {
      final PortMapping mapping = e.getValue();
      final PortBinding binding = new PortBinding();
      if (mapping.getExternalPort() == null) {
        binding.hostPort = ports.get(e.getKey()).toString();
      } else {
        binding.hostPort = mapping.getExternalPort().toString();
      }
      final String entry = containerPort(mapping.getInternalPort(), mapping.getProtocol());
      bindings.put(entry, asList(binding));
    }
    return bindings;
  }



  /**
   * Compute docker container environment variables.
   */
  private String[] containerEnv(final Job descriptor) {
    final Map<String, String> env = getContainerEnvMap(descriptor);

    final List<String> envList = Lists.newArrayList();
    for (final Map.Entry<String, String> entry : env.entrySet()) {
      envList.add(entry.getKey() + '=' + entry.getValue());
    }

    return envList.toArray(new String[envList.size()]);
  }

  private Map<String, String> getContainerEnvMap(final Job descriptor) {
    final Map<String, String> env = Maps.newHashMap(envVars);
    // Job environment variables take precedence.
    env.putAll(descriptor.getEnv());
    return env;
  }

  private static String containerName(final JobId id) {
    final String random = Integer.toHexString(new SecureRandom().nextInt());
    return id.toShortString().replace(':', '_') + "_" + random;
  }

  /**
   * A runner service that runs a container once.
   */
  private class Runner extends InterruptingExecutionThreadService implements Service {

    private final long delayMillis;
    private final SettableFuture<Integer> resultFuture = SettableFuture.create();

    private ListenableFuture<Void> startFuture;
    private volatile InputStream pullStream;

    public Runner(final long delayMillis) {
      this.delayMillis = delayMillis;
    }

    public ListenableFuture<Integer> result() {
      return resultFuture;
    }

    private List<RegistrationHandle> namelessRegister(Map<String, PortMapping> ports)
        throws InterruptedException {
      if (registrar == null) {
        return null;
      }

      final List<ListenableFuture<RegistrationHandle>> futures = Lists.newArrayList();
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
          if (mapping.getExternalPort() == null) {
            log.error("no external '{}' port for registration: '{}'", portName, registration);
            continue;
          }
          futures.add(registrar.register(registration.getName(), registration.getProtocol(),
                                         mapping.getExternalPort()));
        }
      }

      try {
        return Futures.allAsList(futures).get();
      } catch (ExecutionException e) {
        log.error("Error registering with nameless", e);
      }
      return null;
    }

    private void namelessDeregister(final List<RegistrationHandle> handles) {
      if (registrar == null) {
        return;
      }

      final List<ListenableFuture<Void>> futures = Lists.newArrayList();
      for (RegistrationHandle handle : handles) {
        futures.add(registrar.unregister(handle));
      }

      try {
        Futures.allAsList(futures).get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error unregistering with nameless", e);
        if (e instanceof InterruptedException) {
          Thread.interrupted();
        }
      }
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public void run() {
      try {
        metrics.supervisorRun();
        // Delay
        Thread.sleep(delayMillis);

        // Get centrally registered status
        final TaskStatus taskStatus = model.getTaskStatus(jobId);
        final String registeredContainerId =
            (taskStatus == null) ? null : taskStatus.getContainerId();

        // Find out if the container is already running
        final ContainerInspectResponse containerInfo =
            getRunningContainerInfo(registeredContainerId);

        // Ensure we have the image
        final String image = job.getImage();
        try {
          setStatus(PULLING_IMAGE, null);
          maybePullImage(image);
        } catch (ImagePullFailedException e) {
          throttle = ThrottleState.IMAGE_PULL_FAILED;
          setStatus(FAILED, null);
          throw e;
        } catch (ImageMissingException e) {
          throttle = ThrottleState.IMAGE_MISSING;
          setStatus(FAILED, null);
          throw e;
        }

        // Create and start container if necessary
        final String containerId;
        if (containerInfo != null && containerInfo.state.running) {
          containerId = registeredContainerId;
        } else {
          containerId = startContainer(image);
        }

        // Expose ports
        final ContainerInspectResponse runningContainerInfo;
        runningContainerInfo = docker.safeInspectContainer(containerId);

        final Map<String, PortMapping> ports = parsePortBindings(runningContainerInfo);
        setStatus(RUNNING, containerId, ports);
        metrics.containersRunning();

        // Wait for container to die
        flapController.jobStarted();
        final List<RegistrationHandle> registrationHandles = namelessRegister(ports);
        final int exitCode;
        try {
          exitCode = docker.waitContainer(containerId);
        } finally {
          namelessDeregister(registrationHandles);
        }
        log.info("container exited: {}: {}: {}", job, containerId, exitCode);
        flapController.jobDied();
        throttle = flapController.isFlapping() ? ThrottleState.FLAPPING : ThrottleState.NO;
        setStatus(EXITED, containerId, ports);
        metrics.containersExited();
        resultFuture.set(exitCode);
      } catch (DockerException e) {
        if (!docker.checkForDockerTimeout(e, "unspecific")) {
          metrics.containersThrewException();
        }
        resultFuture.setException(e);
      } catch (InterruptedException e) {
        metrics.containersThrewException();
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

    private String startContainer(final String image)
        throws InterruptedException, DockerException {
      setStatus(CREATING, null);
      final ContainerConfig containerConfig = containerConfig(job);

      commandWrapper.modifyCreateConfig(image, job, docker.safeInspectImage(image), containerConfig);

      final String name = containerName(job.getId());
      final ContainerCreateResponse container;
      container = docker.createContainer(containerConfig, name);

      final String containerId = container.id;
      log.info("created container: {}: {}, {}", job, container, containerConfig);

      final HostConfig hostConfig = hostConfig();
      commandWrapper.modifyStartConfig(hostConfig);

      setStatus(STARTING, containerId, null);
      log.info("starting container: {}: {} {}", job, containerId, hostConfig);
      docker.startContainer(containerId, hostConfig);
      log.info("started container: {}: {}", job, containerId);
      metrics.containerStarted();
      return containerId;
    }

    private ContainerInspectResponse getRunningContainerInfo(final String registeredContainerId)
        throws DockerException {
      final ContainerInspectResponse containerInfo;
      if (registeredContainerId != null) {
        log.info("inspecting container: {}: {}", job, registeredContainerId);
        containerInfo = docker.safeInspectContainer(registeredContainerId);
      } else {
        containerInfo = null;
      }
      return containerInfo;
    }

    private void maybePullImage(final String image)
        throws DockerException, InterruptedException, ImagePullFailedException,
               ImageMissingException {
      if (imageExists(image)) {
        metrics.imageCacheHit();
        return;
      }
      final MetricsContext context = metrics.containerPull();
      final PullClientResponse response;
      try {
        response = docker.pull(image);
        context.success();
      } catch (InterruptedException e) {
        // may be overclassifying user errors as failures here
        context.failure();
        throw new ImagePullFailedException(e);
      }
      pullStream = response.getResponse().getEntityInputStream();

      try {
        // Wait until image is completely pulled
        tailPull(image, pullStream);
      } catch (PullingException e) {
        context.failure();
        throw new ImagePullFailedException(e);
      } finally {
        response.close();
      }
    }

    private boolean imageExists(final String image) throws DockerException, InterruptedException {
      try {
        return docker.safeInspectImage(image) != null;
      } catch (DockerException e) {
        if (e.getMessage().contains("No such image")) {
          return false;
        } else {
          throw e;
        }
      }
    }

    public void disrupt() {
      // Close the pull stream as it doesn't respond to thread interrupts
      final InputStream stream = pullStream;
      if (stream != null) {
        try {
          stream.close();
        } catch (Exception e) {
          // XXX (dano): catch Exception here as the guts of pullStream.close() might throw NPE.
          log.debug("exception when closing pull feedback stream", e);
        }
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

  private Map<String, PortMapping> parsePortBindings(final ContainerInspectResponse info) {
    if (info.networkSettings.ports == null) {
      return emptyMap();
    }
    return parsePortBindings(info.networkSettings.ports);
  }

  private Map<String, PortMapping> parsePortBindings(final Map<String, List<PortBinding>> ports) {
    final ImmutableMap.Builder<String, PortMapping> builder = ImmutableMap.builder();
    for (final Map.Entry<String, List<PortBinding>> e : ports.entrySet()) {
      final PortMapping mapping = parsePortBinding(e.getKey(), e.getValue());
      final String name = getPortNameForPortNumber(mapping.getInternalPort());
      if (name == null) {
        log.info("got internal port unknown to the job: {}", mapping.getInternalPort());
      } else if (mapping.getExternalPort() == null) {
        log.debug("unbound port: {}/{}", name, mapping.getInternalPort());
      } else {
        builder.put(name, mapping);
      }
    }
    return builder.build();
  }

  /**
   * Assumes port binding matches output of {@link #portBindings}
   */
  private PortMapping parsePortBinding(final String entry, final List<PortBinding> bindings) {
    final List<String> parts = Splitter.on('/').splitToList(entry);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid port binding: " + entry);
    }

    final String protocol = parts.get(1);

    final int internalPort;
    try {
      internalPort = Integer.parseInt(parts.get(0));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid port binding: " + entry, ex);
    }

    if (bindings == null) {
      return PortMapping.of(internalPort);
    } else {
      if (bindings.size() != 1) {
        throw new IllegalArgumentException("Expected single binding, got " + bindings.size());
      }

      final PortBinding binding = bindings.get(0);
      final int externalPort;
      try {
        externalPort = Integer.parseInt(binding.hostPort);
      } catch (NumberFormatException e1) {
        throw new IllegalArgumentException("Invalid host port: " + binding.hostPort);
      }
      return PortMapping.of(internalPort, externalPort, protocol);
    }
  }

  private String getPortNameForPortNumber(final int internalPort) {
    for (final Entry<String, PortMapping> portMapping : job.getPorts().entrySet()) {
      if (portMapping.getValue().getInternalPort() == internalPort) {
        log.info("found mapping for internal port {} {} -> {}",
            internalPort,
            portMapping.getValue().getInternalPort(),
            portMapping.getKey());
        return portMapping.getKey();
      }
    }
    return null;
  }

  private void tailPull(final String image, final InputStream stream)
      throws ImagePullFailedException, ImageMissingException, PullingException {

    final MappingIterator<Map<String, Object>> messages;
    try {
      messages = Json.readValues(stream, new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // TODO (dano): this can block forever and seems to be impossible to abort by closing the client
    while (true) {
      if (!hasNext(messages)) {
        break;
      }
      Map<String, Object> message = getNext(messages);
      final Object error = message.get("error");
      if (error != null) {
        if (error.toString().contains("404")) {
          throw new ImageMissingException(message.toString());
        } else {
          throw new ImagePullFailedException(message.toString());
        }
      }
      log.info("pull {}: {}", image, message);
    }
  }

  private Map<String, Object> getNext(final MappingIterator<Map<String, Object>> messages)
      throws PullingException {
    final ListenableFuture<Map<String, Object>> future = pullStreamExecutor.submit(
      new Callable<Map<String, Object>>() {
        @Override
        public Map<String, Object> call() throws Exception {
          return messages.next();
        }
      });
    try {
      return Futures.get(future, PULL_POLL_TIMEOUT_SECONDS, SECONDS, PullingException.class);
    } catch (PullingException e) {
      future.cancel(true);
      throw e;
    }
  }

  private boolean hasNext(final MappingIterator<Map<String, Object>> messages)
      throws PullingException {
    final ListenableFuture<Boolean> future = pullStreamExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return messages.hasNext();
      }
    });
    try {
      return Futures.get(future, PULL_POLL_TIMEOUT_SECONDS, SECONDS, PullingException.class);
    } catch (PullingException e) {
      future.cancel(true);
      throw e;
    }
  }

  private static class ImageMissingException extends Exception {

    private ImageMissingException(final String message) {
      super(message);
    }
  }

  private static class ImagePullFailedException extends Exception {

    private ImagePullFailedException(final Throwable cause) {
      super(cause);
    }

    private ImagePullFailedException(final String message) {
      super(message);
    }
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run() throws InterruptedException {
      final boolean done = performedCommand == currentCommand;
      currentCommand.perform(done);
      if (!done) {
        performedCommand = currentCommand;
        fireStateChanged();
      }
    }
  }

  private void fireStateChanged() {
    if (listener == null) {
      return;
    }
    try {
      listener.stateChanged(this);
    } catch (Exception e) {
      log.error("Listener threw exception", e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Map<String, Integer> ports;

    private Builder() {
    }

    private JobId jobId;
    private Job job;
    private AgentModel model;
    private AsyncDockerClient dockerClient;
    private Map<String, String> envVars = emptyMap();
    private FlapController flapController;
    private RestartPolicy restartPolicy;
    private TaskStatusManager stateManager;
    private NamelessRegistrar registrar;
    private CommandWrapper commandWrapper;
    private String host;
    private SupervisorMetrics metrics;
    private RiemannFacade riemannFacade;
    private Listener listener;

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setRestartPolicy(final RestartPolicy restartPolicy) {
      this.restartPolicy = restartPolicy;
      return this;
    }

    public Builder setJob(final Job job) {
      this.job = job;
      return this;
    }

    public Builder setModel(final AgentModel model) {
      this.model = model;
      return this;
    }

    public Builder setDockerClient(final AsyncDockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    public Builder setEnvVars(Map<String, String> envVars) {
      this.envVars = envVars;
      return this;
    }

    public Builder setFlapController(FlapController flapController) {
      this.flapController = flapController;
      return this;
    }

    public Builder setTaskStatusManager(final TaskStatusManager manager) {
      stateManager = manager;
      return this;
    }

    public Builder setNamelessRegistrar(final @Nullable NamelessRegistrar registrar) {
      this.registrar = registrar;
      return this;
    }

    public Builder setCommandWrapper(final CommandWrapper commandWrapper) {
      this.commandWrapper = commandWrapper;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setMetrics(SupervisorMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder setRiemannFacade(RiemannFacade riemannFacade) {
      this.riemannFacade = riemannFacade;
      return this;
    }

    public Builder setPorts(final Map<String, Integer> ports) {
      this.ports = ports;
      return this;
    }

    public Builder setListener(final Listener listener) {
      this.listener = listener;
      return this;
    }

    public Supervisor build() {
      return new Supervisor(this);
    }
  }

  private interface Command {

    void perform(final boolean done);
  }

  private class Start implements Command {

    @Override
    public void perform(final boolean done) {
      if (runner == null) {
        startAfter(0);
        return;
      }
      if (!runner.isRunning()) {
        if (!runner.result().isDone()) {
          log.warn("runner not running but result future not done!");
          startAfter(restartPolicy.restartThrottle(throttle));
          return;
        }
        final Result<Integer> result = Result.of(runner.result());
        if (result.isSuccess()) {
          startAfter(restartPolicy.restartThrottle(throttle));
        } else {
          final Throwable t = result.getException();
          if (t instanceof InterruptedException || t instanceof InterruptedIOException) {
            log.debug("task runner interrupted");
          } else {
            log.error("task runner threw exception", t);
          }
          long restartDelay = restartPolicy.getRetryIntervalMillis();
          long throttleDelay = restartPolicy.restartThrottle(throttle);
          startAfter(Math.max(restartDelay, throttleDelay));
        }
      }
    }

    private void startAfter(final long delay) {
      log.debug("starting job: {} (delay={}): {}", jobId, delay, job);
      runner = new Runner(delay);
      runner.startAsync();
      runner.result().addListener(reactor.signalRunnable(), sameThreadExecutor());
    }
  }

  private class Stop implements Command {

    @Override
    public void perform(final boolean done) {
      if (done) {
        return;
      }

      log.debug("stopping job: id={}: job={}", jobId, job);

      // Stop the runner
      if (runner != null) {
        runner.stopAsync();
      }

      final TaskStatus taskStatus = model.getTaskStatus(jobId);
      final String containerId = (taskStatus == null) ? null : taskStatus.getContainerId();

      if (containerId == null) {
        setStatus(STOPPED, null);
        return;
      }

      final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
          .setMinIntervalMillis(SECONDS.toMillis(5))
          .setMaxIntervalMillis(SECONDS.toMillis(30))
          .build().newScheduler();

      // Wait for the runner and container to die
      boolean containerStopped = false;
      while ((runner != null && runner.isRunning()) || !containerStopped) {
        if (!containerStopped) {
          // See if the container is running
          ContainerInspectResponse containerInfo = null;
          try {
            containerInfo = docker.safeInspectContainer(containerId);
            if (containerInfo == null || !containerInfo.state.running) {
              containerStopped = true;
            }
          } catch (DockerException e) {
            log.error("failed to query container {}", containerId, e);
            sleepUninterruptibly(retryScheduler.nextMillis(), MILLISECONDS);
          }

          // Kill the container if it's running
          if (containerInfo != null && containerInfo.state != null &&
              containerInfo.state.running) {
            try {
              docker.kill(containerId);
              break;
            } catch (DockerException | InterruptedException e) {
              log.error("failed to kill container {}", containerId, e);
              sleepUninterruptibly(retryScheduler.nextMillis(), MILLISECONDS);
            }
          }
        }

        // Disrupt work in progress to speed the runner to it's demise
        if (runner != null) {
          runner.disrupt();
        }

        sleepUninterruptibly(1, SECONDS);
      }

      runner = null;

      setStatus(STOPPED, containerId);
    }
  }

  private class Nop implements Command {

    @Override
    public void perform(final boolean done) {
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("jobId", jobId)
        .add("job", job)
        .add("envVars", envVars)
        .add("host", host)
        .add("ports", ports)
        .add("currentCommand", currentCommand)
        .add("performedCommand", performedCommand)
        .toString();
  }
}
