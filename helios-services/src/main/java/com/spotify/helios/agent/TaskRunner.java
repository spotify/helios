/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.agent;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerTimeoutException;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.exceptions.ImagePullFailedException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.serviceregistration.NopServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrationHandle;
import com.spotify.helios.servicescommon.InterruptingExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runner service that starts a container once.
 */
class TaskRunner extends InterruptingExecutionThreadService {

  private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

  private final long delayMillis;
  private final SettableFuture<Integer> result = SettableFuture.create();
  private final TaskConfig config;
  private final DockerClient docker;
  private final String existingContainerId;
  private final Listener listener;
  private final ServiceRegistrar registrar;
  private final Optional<HealthChecker> healthChecker;
  private Optional<ServiceRegistrationHandle> serviceRegistrationHandle;
  private Optional<String> containerId;
  private final String containerName;
  private int secondsToWaitBeforeKill;

  private TaskRunner(final Builder builder) {
    super("TaskRunner(" + builder.taskConfig.name() + ")");
    this.delayMillis = builder.delayMillis;
    this.config = checkNotNull(builder.taskConfig, "config");
    this.containerName = config.containerName();
    this.docker = checkNotNull(builder.docker, "docker");
    this.listener = checkNotNull(builder.listener, "listener");
    this.existingContainerId = builder.existingContainerId;
    this.registrar = checkNotNull(builder.registrar, "registrar");
    this.secondsToWaitBeforeKill = checkNotNull(builder.secondsToWaitBeforeKill, "waitBeforeKill");
    this.healthChecker = Optional.fromNullable(builder.healthChecker);
    this.serviceRegistrationHandle = Optional.absent();
    this.containerId = Optional.absent();
  }

  public Result<Integer> result() {
    return Result.of(result);
  }

  public ListenableFuture<Integer> resultFuture() {
    return result;
  }

  /**
   * Unregister a set of service endpoints previously registered.
   *
   * @return boolean true if service registration handle was present, false otherwise
   */
  public boolean unregister() {
    if (serviceRegistrationHandle.isPresent()) {
      registrar.unregister(serviceRegistrationHandle.get());
      serviceRegistrationHandle = Optional.absent();
      return true;
    }
    return false;
  }

  /**
   * Stops this container.
   */
  public void stop() throws InterruptedException {
    // Tell docker to stop or eventually kill the container
    final String container = containerId.or(containerName);

    // Interrupt the thread blocking on waitContainer
    stopAsync().awaitTerminated();

    try {
      docker.stopContainer(container, secondsToWaitBeforeKill);
    } catch (DockerException e) {
      if ((e instanceof ContainerNotFoundException) && !containerId.isPresent()) {
        // we tried to stop the container by name but no container of the given name existed.
        // this isn't surprising or exceptional, just means the container wasn't started yet.
      } else {
        log.warn("Stopping container {} failed", container, e);
      }
    }
  }

  protected String getContainerError() {
    final ContainerInfo info;
    try {
      // If we don't know our containerId at this point there's not a lot we can do.
      info = getContainerInfo(containerId.orNull());
    } catch (DockerException | InterruptedException e) {
      log.warn("failed to propagate container error: {}", e);
      return "";
    }
    if (info == null) {
      return "";
    }
    return info.state().error();
  }

  @Override
  protected void run() {
    try {
      final int exitCode = run0();
      result.set(exitCode);
    } catch (Exception e) {
      listener.failed(e, getContainerError());
      result.setException(e);
    }
  }

  private int run0() throws InterruptedException, DockerException {
    // Delay
    Thread.sleep(delayMillis);

    // Check if the container is already running
    final ContainerInfo info = getContainerInfo(existingContainerId);
    final String containerId;

    if (info != null && info.state().running()) {
      containerId = existingContainerId;
      this.containerId = Optional.of(existingContainerId);
    } else {
      // Create and start container if necessary
      containerId = createAndStartContainer();
      this.containerId = Optional.of(containerId);

      if (healthChecker.isPresent()) {
        listener.healthChecking();

        final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
            .setMinIntervalMillis(SECONDS.toMillis(1))
            .setMaxIntervalMillis(SECONDS.toMillis(30))
            .build().newScheduler();

        while (!healthChecker.get().check(containerId)) {
          final ContainerState state = getContainerState(containerId);
          if (state == null) {
            final String err = "container " + containerId + " was not found during health "
                               + "checking, or has no State object";
            log.warn(err);
            throw new RuntimeException(err);
          }
          if (!state.running()) {
            final String err = "container " + containerId + " exited during health checking. "
                               + "Exit code: " + state.exitCode() + ", Config: " + config;
            log.warn(err);
            throw new RuntimeException(err);
          }

          final long retryMillis = retryScheduler.nextMillis();
          log.warn("container failed healthcheck, will retry in {}ms: {}: {}",
              retryMillis, config, containerId);
          Thread.sleep(retryMillis);
        }

        log.info("healthchecking complete of containerId={} taskConfig={}", containerId, config);
      } else {
        log.info("no healthchecks configured for containerId={} taskConfig={}",
            containerId, config);
      }
    }

    listener.running();

    // Register and wait for container to exit
    serviceRegistrationHandle = Optional.fromNullable(registrar.register(config.registration()));
    final ContainerExit exit;
    try {
      exit = docker.waitContainer(containerId);
    } finally {
      unregister();
      this.containerId = Optional.absent();
    }

    log.info("container exited: {}: {}: {}", config, containerId, exit.statusCode());
    listener.exited(exit.statusCode());

    return exit.statusCode();
  }

  private String createAndStartContainer()
      throws DockerException, InterruptedException {

    // Ensure we have the image
    boolean serializePulls = false;
    final Optional<String> dockerVersion = tryGetDockerVersion();
    if (dockerVersion.isPresent()) {
      final String version = dockerVersion.get();
      if (version.startsWith("1.6.") || version.startsWith("1.7.") || version.startsWith("1.8.")) {
        // Docker versions 1.6 through 1.8 have issues with concurrent pulls
        serializePulls = true;
      }
    }

    final String image = config.containerImage();
    if (serializePulls) {
      synchronized (docker) {
        pullImage(image);
      }
    } else {
      pullImage(image);
    }

    return startContainer(image, dockerVersion);
  }

  private String startContainer(final String image, final Optional<String> dockerVersion)
      throws InterruptedException, DockerException {

    // Get container image info
    final ImageInfo imageInfo = docker.inspectImage(image);
    if (imageInfo == null) {
      throw new HeliosRuntimeException("docker inspect image returned null on image " + image);
    }

    // Create container
    final HostConfig hostConfig = config.hostConfig(dockerVersion);
    final ContainerConfig containerConfig = config.containerConfig(imageInfo, dockerVersion)
        .toBuilder()
        .hostConfig(hostConfig)
        .build();
    listener.creating();
    final ContainerCreation container = docker.createContainer(containerConfig, containerName);
    log.info("created container: {}: {}, {}", config, container, containerConfig);
    listener.created(container.id());

    // Start container
    log.info("starting container: {}: {} {}", config, container.id(), hostConfig);
    listener.starting();
    docker.startContainer(container.id());
    log.info("started container: {}: {}", config, container.id());
    listener.started();

    return container.id();
  }

  private ContainerInfo getContainerInfo(final String existingContainerId)
      throws DockerException, InterruptedException {
    if (existingContainerId == null) {
      return null;
    }
    log.info("inspecting container: {}: {}", config, existingContainerId);
    try {
      return docker.inspectContainer(existingContainerId);
    } catch (ContainerNotFoundException e) {
      return null;
    }
  }

  private ContainerState getContainerState(final String existingContainerId)
      throws DockerException, InterruptedException {
    final ContainerInfo info = getContainerInfo(existingContainerId);
    if (info == null) {
      return null;
    }
    return info.state();
  }

  private Optional<String> tryGetDockerVersion() {
    try {
      return Optional.fromNullable(docker.version().version());
    } catch (Exception e) {
      log.error("couldn't fetch Docker version: {}", e);
      return Optional.absent();
    }
  }

  private void pullImage(final String image) throws DockerException, InterruptedException {
    listener.pulling();

    DockerTimeoutException wasTimeout = null;
    final Stopwatch pullTime = Stopwatch.createStarted();

    // Attempt to pull.  Failure, while less than ideal, is ok.
    try {
      docker.pull(image);
      listener.pulled();
      log.info("Pulled image {} in {}s", image, pullTime.elapsed(SECONDS));
    } catch (DockerTimeoutException e) {
      log.warn("Pulling image {} failed with timeout after {}s", image,
          pullTime.elapsed(SECONDS), e);
      listener.pullFailed();
      wasTimeout = e;
    } catch (DockerException e) {
      log.warn("Pulling image {} failed after {}s", image, pullTime.elapsed(SECONDS), e);
      listener.pullFailed();
    }

    try {
      // If we don't have the image by now, fail.
      docker.inspectImage(image);
    } catch (ImageNotFoundException e) {
      // If we get not found, see if we timed out above, since that's what we actually care
      // to know, as the pull should have fixed the not found-ness.
      if (wasTimeout != null) {
        throw new ImagePullFailedException("Failed pulling image " + image + " because of timeout",
            wasTimeout);
      }
      throw e;
    }
  }

  public interface Listener {

    void failed(Throwable th, String containerError);

    void pulling();

    void pulled();

    void pullFailed();

    void creating();

    void created(String containerId);

    void starting();

    void started();

    void healthChecking();

    void running();

    void exited(int code);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private long delayMillis;
    private TaskConfig taskConfig;
    private DockerClient docker;
    private String existingContainerId;
    private Listener listener;
    private HealthChecker healthChecker;
    private int secondsToWaitBeforeKill;
    public ServiceRegistrar registrar = new NopServiceRegistrar();

    public Builder delayMillis(final long delayMillis) {
      this.delayMillis = delayMillis;
      return this;
    }

    public Builder config(final TaskConfig config) {
      this.taskConfig = config;
      return this;
    }

    public Builder docker(final DockerClient docker) {
      this.docker = docker;
      return this;
    }

    public Builder existingContainerId(final String existingContainerId) {
      this.existingContainerId = existingContainerId;
      return this;
    }

    public Builder listener(final Listener listener) {
      this.listener = listener;
      return this;
    }

    public Builder healthChecker(final HealthChecker healthChecker) {
      this.healthChecker = healthChecker;
      return this;
    }

    public Builder registrar(final ServiceRegistrar registrar) {
      this.registrar = registrar;
      return this;
    }

    public Builder secondsToWaitBeforeKill(int seconds) {
      this.secondsToWaitBeforeKill = seconds;
      return this;
    }

    public TaskRunner build() {
      return new TaskRunner(this);
    }
  }

  public static class NopListener implements Listener {

    @Override
    public void failed(final Throwable th, final String containerError) {

    }

    @Override
    public void pulling() {

    }

    @Override
    public void pulled() {

    }

    @Override
    public void pullFailed() {

    }

    @Override
    public void creating() {

    }

    @Override
    public void created(final String containerId) {

    }

    @Override
    public void starting() {

    }

    @Override
    public void started() {

    }

    @Override
    public void healthChecking() {

    }

    @Override
    public void running() {

    }

    @Override
    public void exited(final int code) {

    }
  }
}
