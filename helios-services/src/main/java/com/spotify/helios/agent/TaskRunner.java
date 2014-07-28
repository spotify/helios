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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.docker.client.ContainerNotFoundException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
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

  private TaskRunner(final Builder builder) {
    super("TaskRunner(" + builder.taskConfig.name() + ")");
    this.delayMillis = builder.delayMillis;
    this.config = checkNotNull(builder.taskConfig, "config");
    this.docker = checkNotNull(builder.docker, "docker");
    this.listener = checkNotNull(builder.listener, "listener");
    this.existingContainerId = builder.existingContainerId;
    this.registrar = checkNotNull(builder.registrar, "registrar");
  }

  public Result<Integer> result() {
    return Result.of(result);
  }

  public ListenableFuture<Integer> resultFuture() {
    return result;
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
    listener.running();

    // Register and wait for container to exit
    final ServiceRegistrationHandle handle = registrar.register(config.registration());
    final ContainerExit exit;
    try {
      exit = docker.waitContainer(containerId);
    } finally {
      registrar.unregister(handle);
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
    docker.pull(image);
  }

  public static interface Listener {

    void failed(Throwable t);

    void pulling();

    void creating();

    void created(String containerId);

    void starting();

    void started();

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
    public void running() {

    }

    @Override
    public void exited(final int code) {

    }
  }
}
