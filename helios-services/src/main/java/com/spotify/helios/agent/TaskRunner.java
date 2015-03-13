/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.docker.client.ContainerNotFoundException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerTimeoutException;
import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.ImagePullFailedException;
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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A runner service that starts a container once.
 */
class TaskRunner extends InterruptingExecutionThreadService {

  private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);
  private static final int SECONDS_TO_WAIT_BEFORE_KILL = 120;

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

  private TaskRunner(final Builder builder) {
    super("TaskRunner(" + builder.taskConfig.name() + ")");
    this.delayMillis = builder.delayMillis;
    this.config = checkNotNull(builder.taskConfig, "config");
    this.docker = checkNotNull(builder.docker, "docker");
    this.listener = checkNotNull(builder.listener, "listener");
    this.existingContainerId = builder.existingContainerId;
    this.registrar = checkNotNull(builder.registrar, "registrar");
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
    if (containerId.isPresent()) {
      final String container = containerId.get();

      // Interrupt the thread blocking on waitContainer
      stopAsync().awaitTerminated();

      // Tell docker to stop or eventually kill the container
      try {
        docker.stopContainer(container, SECONDS_TO_WAIT_BEFORE_KILL);
      } catch (DockerException e) {
        log.warn("Stopping container {} failed", container, e);
      }
    }
  }

  @Override
  protected void run() {
    try {
      final int exitCode = run0();
      result.set(exitCode);
    } catch (Exception e) {
      listener.failed(e);
      result.setException(e);
    }
  }

  private int run0() throws InterruptedException, DockerException {
    // Delay
    Thread.sleep(delayMillis);

    // Create and start container if necessary
    final String containerId = createAndStartContainer();
    this.containerId = Optional.of(containerId);

    if (healthChecker.isPresent()) {
      listener.healthChecking();

      final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
          .setMinIntervalMillis(SECONDS.toMillis(1))
          .setMaxIntervalMillis(SECONDS.toMillis(30))
          .build().newScheduler();

      while (!healthChecker.get().check(containerId)) {
        final ContainerState state = docker.inspectContainer(containerId).state();
        if (!state.running()) {
          log.warn("container exited during health checking: {}: {}: {}",
                   config, containerId, state.exitCode());
          throw new RuntimeException("container exited during health checking");
        }

        final long retryMillis = retryScheduler.nextMillis();
        log.warn("container failed healthcheck, will retry in {}ms: {}: {}",
                 retryMillis, config, containerId);
        Thread.sleep(retryMillis);
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

    // Check if the container is already running
    final ContainerInfo info = getContainerInfo(existingContainerId);
    if (info != null && info.state().running()) {
      return existingContainerId;
    }

    // Ensure we have the image
    final String image = config.containerImage();
    pullImage(image);

    return startContainer(image);
  }

  private String startContainer(final String image)
      throws InterruptedException, DockerException {

    // Get container image info
    final ImageInfo imageInfo = docker.inspectImage(image);
    if (imageInfo == null) {
      throw new HeliosRuntimeException("docker inspect image returned null on image " + image);
    }

    // Create container
    final ContainerConfig containerConfig = config.containerConfig(imageInfo);
    final String name = config.containerName();
    listener.creating();
    final ContainerCreation container = docker.createContainer(containerConfig, name);
    log.info("created container: {}: {}, {}", config, container, containerConfig);
    listener.created(container.id());

    // Start container
    final HostConfig hostConfig = config.hostConfig();
    log.info("starting container: {}: {} {}", config, container.id(), hostConfig);
    listener.starting();
    docker.startContainer(container.id(), hostConfig);
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

  private void pullImage(final String image) throws DockerException, InterruptedException {
    listener.pulling();

    DockerTimeoutException wasTimeout = null;
    // Attempt to pull.  Failure, while less than ideal, is ok.
    try {
      docker.pull(image);
      listener.pulled();
    } catch (DockerTimeoutException e) {
      log.warn("Pulling image {} failed with timeout", image, e);
      listener.pullFailed();
      wasTimeout = e;
    } catch (DockerException e) {
      log.warn("Pulling image {} failed", image, e);
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

  public static interface Listener {

    void failed(Throwable t);

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

    public TaskRunner build() {
      return new TaskRunner(this);
    }
  }

  public static class NopListener implements Listener {

    @Override
    public void failed(final Throwable t) {

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
