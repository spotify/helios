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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
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

  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newCachedThreadPool(), 0, SECONDS));

  private final long delayMillis;
  private final SettableFuture<Integer> resultFuture = SettableFuture.create();
  private final ServiceRegistrar registrar;
  private final Job job;
  private final TaskConfig taskConfig;
  private final SupervisorMetrics metrics;
  private final DockerClient docker;
  private final FlapController flapController;
  private final AtomicReference<ThrottleState> throttle;
  private final StatusUpdater statusUpdater;
  private final String existingContainerId;

  private ListenableFuture<Void> startFuture;

  public TaskRunner(final long delayMillis,
                    final ServiceRegistrar registrar,
                    final Job job,
                    final TaskConfig taskConfig,
                    final SupervisorMetrics metrics,
                    final DockerClient docker,
                    final FlapController flapController,
                    final AtomicReference<ThrottleState> throttle,
                    final StatusUpdater statusUpdater,
                    final String existingContainerId) {
    super("TaskRunner(" + job.toString() + ")");
    this.delayMillis = delayMillis;
    this.registrar = registrar;
    this.job = job;
    this.taskConfig = taskConfig;
    this.metrics = metrics;
    this.docker = docker;
    this.flapController = flapController;
    this.throttle = throttle;
    this.statusUpdater = statusUpdater;
    this.existingContainerId = existingContainerId;
  }

  @SuppressWarnings("TryWithIdenticalCatches")
  @Override
  public void run() {
    try {
      metrics.supervisorRun();

      // Delay
      Thread.sleep(delayMillis);

      // Create and start container if necessary
      final String containerId = createAndStartContainer();
      statusUpdater.setStatus(RUNNING);

      // Report
      metrics.containersRunning();

      // Wait for container to die
      flapController.jobStarted();
      final ServiceRegistrationHandle registrationHandle =
          serviceRegister(taskConfig.ports());
      final int exitCode;
      try {
        exitCode = flapController.waitFuture(waitContainer(containerId)).statusCode();
      } finally {
        serviceDeregister(registrationHandle);
      }
      log.info("container exited: {}: {}: {}", job, containerId, exitCode);
      flapController.jobDied();
      throttle.set(flapController.isFlapping() ? ThrottleState.FLAPPING : ThrottleState.NO);
      statusUpdater.setStatus(EXITED);
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

  private String createAndStartContainer()
      throws DockerException, InterruptedException {

    // Check if the container is already running
    final ContainerInfo info = getContainerInfo(existingContainerId);
    if (info != null && info.state().running()) {
      return existingContainerId;
    }

    // Ensure we have the image
    final String image = job.getImage();
    try {
      pullImage(image);
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
        final Integer externalPort = mapping.getExternalPort();
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

    final ImageInfo imageInfo = docker.inspectImage(image);
    if (imageInfo == null) {
      throw new HeliosRuntimeException("docker inspect image returned null on image " + image);
    }

    // Create container
    final ContainerConfig containerConfig = taskConfig.containerConfig(imageInfo);
    final String name = taskConfig.containerName();
    final ContainerCreation container = docker.createContainer(containerConfig, name);
    log.info("created container: {}: {}, {}", job, container, containerConfig);

    // Start container
    final HostConfig hostConfig = taskConfig.hostConfig();
    statusUpdater.setStatus(STARTING, container.id());
    log.info("starting container: {}: {} {}", job, container.id(), hostConfig);
    startFuture = startContainer(container.id(), hostConfig);
    try {
      startFuture.get();
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
    log.info("started container: {}: {}", job, container.id());
    metrics.containerStarted();
    return container.id();
  }

  private ContainerInfo getContainerInfo(final String existingContainerId)
      throws DockerException, InterruptedException {
    if (existingContainerId == null) {
      return null;
    }
    log.info("inspecting container: {}: {}", job, existingContainerId);
    try {
      return docker.inspectContainer(existingContainerId);
    } catch (ContainerNotFoundException e) {
      return null;
    }
  }

  private void pullImage(final String image) throws DockerException, InterruptedException {
    try {
      docker.inspectImage(image);
      metrics.imageCacheHit();
      return;
    } catch (ImageNotFoundException ignore) {
      metrics.imageCacheMiss();
    }
    final MetricsContext metric = metrics.containerPull();
    try {
      statusUpdater.setStatus(PULLING_IMAGE);
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
