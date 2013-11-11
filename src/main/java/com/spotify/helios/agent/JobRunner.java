/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.Image;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.coordination.DockerClientFactory;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobGoal;
import com.spotify.helios.common.descriptors.JobStatus;
import com.sun.jersey.api.client.ClientResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.spotify.helios.common.descriptors.JobStatus.State.CREATED;
import static com.spotify.helios.common.descriptors.JobStatus.State.DESTROYED;
import static com.spotify.helios.common.descriptors.JobStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STARTING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class JobRunner {

  private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

  private final State state;
  private final String name;
  private final JobDescriptor descriptor;
  private final DockerClientFactory dockerClientFactory;
  private final Runner runner;
  private final ContainerConfig containerConfig;

  private volatile boolean stop;
  private volatile String containerId;
  private volatile JobGoal goal;


  /**
   * Create a new job runer.
   *
   * @param descriptor The job descriptor.
   * @param state      The worker state to use.
   */
  public JobRunner(final String name, final JobDescriptor descriptor,
                   final State state, final DockerClientFactory dockerClientFactory) {
    this.name = name;
    this.descriptor = descriptor;
    this.containerConfig = containerConfig(descriptor);
    this.state = checkNotNull(state);
    this.dockerClientFactory = dockerClientFactory;
    this.runner = new Runner();
  }

  private ContainerConfig containerConfig(final JobDescriptor descriptor) {
    final ContainerConfig containerConfig = new ContainerConfig();
    containerConfig.setImage(descriptor.getImage());
    final List<String> command = descriptor.getCommand();
    containerConfig.setCmd(command.toArray(new String[command.size()]));
    return containerConfig;
  }

  /**
   * Start job runner.
   */
  public void start(final JobGoal goal) {
    log.debug("starting job runner: descriptor={} goal={}", descriptor, goal);
    this.goal = goal;
    runner.start();
  }

  public void stop() {
    log.debug("stopping container: {}: {}", descriptor, containerId);

    // Let the runner know that it should stop
    stop = true;

    // Wait for it to die
    while (runner.isAlive()) {
      if (containerId != null) {
        final DockerClient dockerClient = dockerClientFactory.create();
        try {
          dockerClient.kill(containerId);
        } catch (DockerException e) {
          log.error("failed to kill container {}", containerId, e);
        }
      }
      log.debug("waiting for runner to die");
      joinUninterruptibly(runner, 1, SECONDS);
    }

    // Destroy the container
    if (containerId != null) {
      try {
        final DockerClient dockerClient = dockerClientFactory.create();
        dockerClient.removeContainer(containerId);
        state.setJobStatus(name, new JobStatus(descriptor, DESTROYED, containerId));
      } catch (DockerException e) {
        log.error("failed to remove container {}", containerId, e);
      }
    }
  }


  public JobDescriptor getDescriptor() {
    return descriptor;
  }

  /**
   * Close this container monitor. The actual container is left as-is.
   */
  public void close() {
    // TODO: it's currently not possible to stop the runner thread without killing the container
    stop = true;
  }

  public JobGoal getGoal() {
    return goal;
  }

  public void setGoal(final JobGoal goal) {
    this.goal = goal;
    // TODO: take some action
  }

  private class Runner extends Thread {

    @Override
    public void run() {
      while (!stop) {
        // TODO: should this be a state machine?
        try {
          final DockerClient dockerClient = dockerClientFactory.create();

          // Check if there's any centrally registered state
          final JobStatus jobStatus = state.getJobStatus(name);
          if (jobStatus != null) {
            containerId = jobStatus.getId();
          }

          // Check if the image exists
          final String image = containerConfig.getImage();
          final List<Image> images = dockerClient.getImages(image);
          if (images.isEmpty()) {
            final ClientResponse pull = dockerClient.pull(image);
            // Wait until image is completely pulled
            jsonTail("pull " + image, pull.getEntityInputStream());
          }

          // Check if container exists
          final ContainerInspectResponse containerInfo;
          if (containerId != null) {
            log.info("inspecting container: {}: {}", descriptor, containerId);
            if (stop) { break; }
            containerInfo = dockerClient.inspectContainer(containerId);
          } else {
            containerInfo = null;
          }

          // Create container if necessary
          if (containerInfo == null) {
            if (stop) { break; }
            final ContainerCreateResponse container = dockerClient.createContainer(containerConfig);
            containerId = container.id;
            if (stop) { break; }
            state.setJobStatus(name, new JobStatus(descriptor, CREATED, containerId));
            log.info("created container: {}: {}", descriptor, container);
          }

          // Start container if necessary
          if (containerInfo == null || !containerInfo.state.running) {
            if (stop) { break; }
            state.setJobStatus(name, new JobStatus(descriptor, STARTING, containerId));
            log.info("starting container: {}: {}", descriptor, containerId);
            if (stop) { break; }
            dockerClient.startContainer(containerId);
            log.info("started container: {}: {}: {}", descriptor, containerId, containerInfo);
          }
          if (stop) { break; }
          state.setJobStatus(name, new JobStatus(descriptor, RUNNING, containerId));

          // Wait for container to die
          if (stop) { break; }
          final int exitCode = dockerClient.waitContainer(containerId);
          log.info("container exited: {}: {}: {}", descriptor, containerId, exitCode);
          if (stop) { break; }
          state.setJobStatus(name, new JobStatus(descriptor, EXITED, containerId));
        } catch (Exception e) {
          if (stop) {
            log.debug("exception during shutdown", e);
          } else {
            // TODO: failure handling
            log.error("exception in container runner", e);
            sleepUninterruptibly(100, MILLISECONDS);
          }
        }
      }
    }

    private void jsonTail(final String operation, final InputStream stream) throws IOException {
      final MappingIterator<Map<String, Object>> messages =
          Json.readValues(stream, new TypeReference<Map<String, Object>>() {});
      while (messages.hasNext()) {
        log.info("{}: {}", operation, messages.next());
      }
    }
  }
}
