/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.Image;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.kpelykh.docker.client.model.PortBinding;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.nameless.client.NamelessRegistrar;
import com.spotify.nameless.client.RegistrationHandle;
import com.sun.jersey.api.client.ClientResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Supervises docker containers for a single job.
 */
class Supervisor {

  private static final Logger log = LoggerFactory.getLogger(Supervisor.class);

  public static final ThreadFactory RUNNER_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-supervisor-runner-%d").build();

  private final Object sync = new Object() {};

  private final AsyncDockerClient docker;

  private final JobId jobId;
  private final Job job;
  private final AgentModel model;
  private final Map<String, String> envVars;
  private final FlapController flapController;
  private final RestartPolicy restartPolicy;
  private final TaskStatusManager stateManager;
  private final NamelessRegistrar registrar;
  private final CommandWrapper commandWrapper;

  private volatile Runner runner;
  private volatile boolean closed;
  private volatile boolean starting;
  private volatile ThrottleState throttle = ThrottleState.NO;

  /**
   * Create a new job supervisor.
   *
   * @param jobId     The job id.
   * @param job       The job.
   * @param model     The model to use.
   * @param envVars   Environment variables to expose to child containers.
   * @param registrar The Nameless registrar to register ports with.
   */
  private Supervisor(final JobId jobId, final Job job,
                     final AgentModel model, final AsyncDockerClient dockerClient,
                     final RestartPolicy restartPolicy, final Map<String, String> envVars,
                     final FlapController flapController, TaskStatusManager stateManager,
                     final @Nullable NamelessRegistrar registrar,
                     final CommandWrapper commandWrapper) {
    this.jobId = checkNotNull(jobId);
    this.job = checkNotNull(job);
    this.model = checkNotNull(model);
    this.docker = checkNotNull(dockerClient);
    this.restartPolicy = checkNotNull(restartPolicy);
    this.envVars = checkNotNull(envVars);
    this.flapController = checkNotNull(flapController);
    this.stateManager = checkNotNull(stateManager);
    this.registrar = registrar;
    this.commandWrapper = checkNotNull(commandWrapper);
  }

  /**
   * Start the job.
   */
  public void start() {
    synchronized (sync) {
      starting = true;
      startJob(0);
    }
  }

  /**
   * Stop the job.
   */
  public void stop() {
    synchronized (sync) {
      starting = false;
      stopJob();
    }
  }

  /**
   * Close this job. The actual container is left as-is.
   */
  public void close() {
    synchronized (sync) {
      closed = true;
      if (runner != null) {
        runner.stop();
      }
    }
  }

  public boolean isStarting() {
    return starting;
  }

  /**
   * Start a {@link Runner} to run the container for a job.
   *
   * Note: sync must be locked when calling this method.
   */
  private void startJob(final long delayMillis) {
    log.debug("start: jobId={}: job={}, closed={}", jobId, job, closed);
    if (closed || !starting) {
      return;
    }
    runner = new RunnerImpl(delayMillis);
    Futures.addCallback(runner.result(), new FutureCallback<Integer>() {
      @Override
      public void onSuccess(final Integer exitCode) {
        synchronized (sync) {
          startJob(restartPolicy.restartThrottle(throttle));
        }
      }

      @Override
      public void onFailure(final Throwable t) {
        if (t instanceof InterruptedException) {
          log.debug("task runner interrupted", t);
        } else {
          log.error("task runner threw exception", t);
        }
        synchronized (sync) {
          startJob(restartPolicy.getRetryIntervalMillis());
        }
      }
    });
  }

  /**
   * Stop the job.
   *
   * Note: sync must be locked when calling this method.
   */
  private void stopJob() {
    log.debug("start: jobId={}: job={}, closed={}", jobId, job, closed);

    // Stop the runner
    if (runner != null) {
      runner.stop();
    }

    final TaskStatus taskStatus = model.getTaskStatus(jobId);
    final String containerId = (taskStatus == null) ? null : taskStatus.getContainerId();

    if (containerId == null) {
      setStatus(STOPPED, containerId);
      return;
    }

    // Kill the job
    while (true) {
      // See if the container is running
      try {
        final ContainerInspectResponse containerInfo = inspectContainer(containerId);
        if (!containerInfo.state.running) {
          break;
        }
      } catch (DockerException e) {
        log.error("failed to query container {}", containerId, e);
        sleepUninterruptibly(100, MILLISECONDS);
        continue;
      }

      // Kill the container
      try {
        Futures.get(docker.kill(containerId), DockerException.class);
        break;
      } catch (DockerException e) {
        log.error("failed to kill container {}", containerId, e);
        sleepUninterruptibly(100, MILLISECONDS);
      }
    }
    setStatus(STOPPED, containerId);
  }

  /**
   * A wrapper around {@link AsyncDockerClient#inspectContainer(String)} that returns null instead
   * of throwing an exception if the container is missing.
   */
  private ContainerInspectResponse inspectContainer(final String containerId)
      throws DockerException {
    // TODO (dano): this or something like it should probably go into the docker client itself
    try {
      return Futures.get(docker.inspectContainer(containerId), DockerException.class);
    } catch (DockerException e) {
      if (e.getCause().getClass() == InterruptedException.class) {
        Thread.interrupted(); // or else we get a cool endless loop of IE's
      }
      // XXX (dano): checking for string in exception message is a kludge
      if (!e.getMessage().contains("No such container")) {
        throw e;
      }
      return null;
    }
  }

  /**
   * A wrapper around {@link AsyncDockerClient#inspectImage(String)} that returns null instead
   * of throwing an exception if the image is missing.
   */
  private ImageInspectResponse inspectImage(final String image)
      throws DockerException {
    // TODO (dano): this or something like it should probably go into the docker client itself
    try {
      return Futures.get(docker.inspectImage(image), DockerException.class);
    } catch (DockerException e) {
      if (e.getCause().getClass() == InterruptedException.class) {
        Thread.interrupted(); // or else we get a cool endless loop of IE's
      }
      // XXX (dano): checking for string in exception message is a kludge
      if (!e.getMessage().contains("No such image")) {
        throw e;
      }
      return null;
    }
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
    stateManager.setStatus(status, flapController.isFlapping(), containerId, ports,
                           getContainerEnvMap(job));
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
    return containerConfig;
  }

  /**
   * Create container port exposure configuration for a job.
   */
  private Map<String, Void> containerExposedPorts(final Job job) {
    final Map<String, Void> ports = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> entry : job.getPorts().entrySet()) {
      final String name = entry.getKey();
      final PortMapping mapping = entry.getValue();
      ports.put(containerPort(name, mapping.getInternalPort()), null);
    }
    return ports;
  }

  /**
   * Create a docker port exposure/mapping entry.
   */
  private String containerPort(final String name, final int port) {
    return port + "/" + name;
  }

  /**
   * Create a container host configuration for a job.
   */
  private HostConfig hostConfig(final Job job) {
    final HostConfig hostConfig = new HostConfig();
    hostConfig.portBindings = portBindings(job);
    return hostConfig;
  }

  /**
   * Create a port binding configuration for a job.
   */
  private Map<String, List<PortBinding>> portBindings(final Job job) {
    final Map<String, List<PortBinding>> ports = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> e : job.getPorts().entrySet()) {
      final String name = e.getKey();
      final PortMapping mapping = e.getValue();
      final PortBinding binding = new PortBinding();
      if (mapping.getExternalPort() != null) {
        binding.hostPort = mapping.getExternalPort().toString();
      }
      final String entry = containerPort(name, mapping.getInternalPort());
      ports.put(entry, asList(binding));
    }
    return ports;
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

  /**
   * Abstract interface for a runner capable of executing a container once.
   */
  interface Runner {

    /**
     * Stop the runner and block until it is terminated. Does not stop the container. Container may
     * be in any state after this method returns.
     */
    void stop();

    /**
     * Get a future holding the container exit code, or an exception if execution failed.
     */
    ListenableFuture<Integer> result();
  }

  /**
   * The concerete implementation of {@link Runner}.
   */
  class RunnerImpl extends AbstractFuture<Integer> implements Runner, Runnable {

    private final ExecutorService executor = newSingleThreadExecutor(RUNNER_THREAD_FACTORY);
    private final long delayMillis;

    private ListenableFuture<Void> startFuture;
    private volatile InputStream pullStream;
    private List<RegistrationHandle> namelessRegistrationHandles;

    public RunnerImpl(final long delayMillis) {
      this.delayMillis = delayMillis;
      executor.execute(this);
    }

    @Override
    public ListenableFuture<Integer> result() {
      return this;
    }

    private void handleNamelessRegistration(Map<String, PortMapping> ports) {
      if (registrar == null) {
        return;
      }

      List<ListenableFuture<RegistrationHandle>> futures = Lists.newArrayList();
      for (Entry<String, PortMapping> entry: ports.entrySet()) {
        final String portName = entry.getKey();
        final PortMapping mapping = entry.getValue();

        if (!(job.getService().equals("") || portName == null)) {
          futures.add(registrar.register(job.getService(), portName, mapping.getExternalPort()));
        }
      }
      try {
        namelessRegistrationHandles = Futures.allAsList(futures).get();
      } catch (InterruptedException | ExecutionException e) {
        // DANO -- Or should we tear down the task when nameless is hosed?  I'm thinking logging
        // and swallowing is *probably* the best choice.
        log.error("Error registering with nameless", e);
        if (e instanceof InterruptedException) {
          Thread.interrupted();
        }
      }
    }

    private void handleNamelessDeregistration() {
      if (registrar == null) {
        return;
      }
      List<ListenableFuture<Void>> futures = Lists.newArrayList();
      for (RegistrationHandle handle : namelessRegistrationHandles) {
        futures.add(registrar.unregister(handle));
      }

      try {
        Futures.allAsList(futures).get();
      } catch (InterruptedException | ExecutionException e) {
        // DANO -- do you have a better idea about what to do if unregistering borks?
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
        maybePullImage(image);

        // Create and start container if necessary
        final String containerId;
        if (containerInfo != null && containerInfo.state.running) {
          containerId = registeredContainerId;
        } else {
          containerId = startContainer(image);
        }

        final Map<String, PortMapping> ports = setUpExposedPorts(containerId);
        setStatus(RUNNING, containerId, ports);

        // Wait for container to die
        final int exitCode = flapController.waitFuture(docker.waitContainer(containerId));
        log.info("container exited: {}: {}: {}", job, containerId, exitCode);
        flapController.jobDied();
        throttle = flapController.isFlapping()
            ? ThrottleState.FLAPPING : ThrottleState.NO;
        setStatus(EXITED, containerId);
        handleNamelessDeregistration();

        set(exitCode);
      } catch (InterruptedException e) {
        setException(e);
      } catch (Exception e) {
        // Keep separate catch clauses to simplify setting breakpoints on actual errors
        setException(e);
      }
    }

    private Map<String, PortMapping> setUpExposedPorts(final String containerId)
        throws InterruptedException, ExecutionException {
      // Look up ports
      final ContainerInspectResponse runningContainerInfo =
          docker.inspectContainer(containerId).get();
      final Map<String, PortMapping> ports = parsePortBindings(runningContainerInfo);
      flapController.jobStarted();
      handleNamelessRegistration(ports);
      return ports;
    }

    private String startContainer(final String image)
        throws InterruptedException, ExecutionException, DockerException {
      setStatus(CREATING, null);
      final ContainerConfig containerConfig = containerConfig(job);

      commandWrapper.modifyCreateConfig(image, job, inspectImage(image), containerConfig);

      final UUID uuid = UUID.randomUUID();
      final String name = job.getId() + ":" + uuid;
      final ContainerCreateResponse container = docker.createContainer(containerConfig, name).get();
      final String containerId = container.id;
      log.info("created container: {}: {}", job, container);

      final HostConfig hostConfig = hostConfig(job);
      commandWrapper.modifyStartConfig(hostConfig);

      setStatus(STARTING, containerId, null);
      log.info("starting container: {}: {}", job, containerId);
      startFuture = docker.startContainer(containerId, hostConfig);
      startFuture.get();
      log.info("started container: {}: {}", job, containerId);
      return containerId;
    }

    private ContainerInspectResponse getRunningContainerInfo(final String registeredContainerId)
        throws DockerException {
      final ContainerInspectResponse containerInfo;
      if (registeredContainerId != null) {
        log.info("inspecting container: {}: {}", job, registeredContainerId);
        containerInfo = inspectContainer(registeredContainerId);
      } else {
        containerInfo = null;
      }
      return containerInfo;
    }

    private void maybePullImage(final String image) throws InterruptedException,
        ExecutionException, IOException {
      final List<Image> images = docker.getImages(image).get();
      if (images.isEmpty()) {
        final ClientResponse pull = docker.pull(image).get();
        // Wait until image is completely pulled
        pullStream = pull.getEntityInputStream();
        jsonTail("pull " + image, pullStream);
      }
    }

    @Override
    public void stop() {
      // Interrupt the thread
      executor.shutdownNow();

      // Await thread termination
      do {
        // Close the pull stream as it doesn't respond to thread interrupts
        if (pullStream != null) {
          try {
            pullStream.close();
          } catch (Exception e) {
            // XXX (dano): catch Exception here as the guts of pullStream.close() might throw NPE.
            log.debug("exception when closing pull feedback stream", e);
          }
        }
        log.debug("runner: waiting to die");
        try {
          get(1, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException ignore) {
        }
      } while (!isDone());

      // Wait for eventual outstanding start request to finish
      if (startFuture != null) {
        try {
          getUninterruptibly(startFuture);
        } catch (ExecutionException e) {
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
      final PortBindingParser parser = new PortBindingParser(e.getKey(), e.getValue());
      final String name = parser.getName();
      final PortMapping mapping = parser.getMapping();
      if (mapping.getExternalPort() == null) {
        log.debug("unbound port: {}/{}", name, mapping.getInternalPort());
        continue;
      }
      builder.put(name, mapping);
    }
    return builder.build();
  }

  /**
   * Tail a json stream until it finishes.
   */
  private static void jsonTail(final String operation, final InputStream stream)
      throws IOException {
    final MappingIterator<Map<String, Object>> messages =
        Json.readValues(stream, new TypeReference<Map<String, Object>>() {});
    while (messages.hasNext()) {
      log.info("{}: {}", operation, messages.next());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private JobId jobId;
    private Job descriptor;
    private AgentModel model;
    private AsyncDockerClient dockerClient;
    private Map<String, String> envVars = emptyMap();
    private FlapController flapController;
    private RestartPolicy restartPolicy;
    private TaskStatusManager stateManager;
    private NamelessRegistrar registrar;
    private CommandWrapper commandWrapper;

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setRestartPolicy(final RestartPolicy restartPolicy) {
      this.restartPolicy = restartPolicy;
      return this;
    }

    public Builder setDescriptor(final Job descriptor) {
      this.descriptor = descriptor;
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

    public Supervisor build() {
      return new Supervisor(jobId, descriptor, model, dockerClient, restartPolicy,
                            envVars, flapController, stateManager, registrar,
                            commandWrapper);
    }
  }

  /**
   * Assumes port binding matches output of {@link #portBindings(Job)}
   */
  private class PortBindingParser {

    private String name;
    private PortMapping mapping;

    public PortBindingParser(final String entry, final List<PortBinding> bindings) {
      final List<String> parts = Splitter.on('/').splitToList(entry);
      if (parts.size() != 2) {
        throw new IllegalArgumentException("Invalid port binding: " + entry);
      }

      this.name = parts.get(1);

      final int internalPort;
      try {
        internalPort = Integer.parseInt(parts.get(0));
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException("Invalid port binding: " + entry, ex);
      }

      if (bindings == null) {
        this.mapping = PortMapping.of(internalPort);
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
        this.mapping = PortMapping.of(internalPort, externalPort);
      }

    }

    public String getName() {
      return name;
    }

    public PortMapping getMapping() {
      return mapping;
    }
  }
}
