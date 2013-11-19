/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

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
import com.kpelykh.docker.client.model.Image;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;
import com.sun.jersey.api.client.ClientResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.spotify.helios.common.descriptors.JobStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.JobStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STOPPED;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Supervises docker containers for a single job.
 */
class Supervisor {

  private static final Logger log = LoggerFactory.getLogger(Supervisor.class);

  private static final long DEFAULT_RESTART_INTERVAL_MILLIS = 100;
  private static final long DEFAULT_RETRY_INTERVAL_MILLIS = 1000;


  public static final ThreadFactory RUNNER_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-supervisor-runner-%d").build();

  private final Object sync = new Object() {};

  private final AsyncDockerClient docker;

  private final String name;
  private final JobDescriptor descriptor;
  private final State state;
  private final long restartIntervalMillis;
  private final long retryIntervalMillis;

  private volatile Runner runner;
  private volatile boolean closed;
  private volatile boolean starting;
  private volatile JobStatus.State status;

  /**
   * Create a new job runner.
   *
   * @param descriptor The job descriptor.
   * @param state      The worker state to use.
   */
  private Supervisor(final String name, final JobDescriptor descriptor,
                     final State state, final AsyncDockerClient dockerClient,
                     final long restartIntervalMillis, final long retryIntervalMillis) {
    this.name = checkNotNull(name);
    this.descriptor = checkNotNull(descriptor);
    this.state = checkNotNull(state);
    this.docker = checkNotNull(dockerClient);
    this.restartIntervalMillis = restartIntervalMillis;
    this.retryIntervalMillis = retryIntervalMillis;
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
    log.debug("start: name={}: descriptor={}, closed={}", name, descriptor, closed);
    if (closed || !starting) {
      return;
    }
    runner = new RunnerImpl(delayMillis);
    Futures.addCallback(runner.result(), new FutureCallback<Integer>() {
      @Override
      public void onSuccess(final Integer exitCode) {
        synchronized (sync) {
          startJob(restartIntervalMillis);
        }
      }

      @Override
      public void onFailure(final Throwable t) {
        if (t instanceof InterruptedException) {
          log.debug("job runner interrupted", t);
        } else {
          log.error("job runner threw exception", t);
        }
        synchronized (sync) {
          startJob(retryIntervalMillis);
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
    log.debug("start: name={}: descriptor={}, closed={}", name, descriptor, closed);

    // Stop the runner
    if (runner != null) {
      runner.stop();
    }

    final JobStatus jobStatus = state.getJobStatus(name);
    final String containerId = (jobStatus == null) ? null : jobStatus.getId();

    // Kill the job
    if (containerId != null) {
      while (true) {

        // See if the container is running
        try {
          final ContainerInspectResponse containerInfo =
              Futures.get(docker.inspectContainer(containerId), DockerException.class);
          if (!containerInfo.state.running) {
            break;
          }
        } catch (DockerException e) {
          // XXX (dano): checking for string in exception message is a kludge
          if (e.getMessage().contains("No such container")) {
            break;
          } else {
            log.error("failed to query container {}", containerId, e);
            sleepUninterruptibly(100, MILLISECONDS);
            continue;
          }
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
    }

    setStatus(STOPPED, containerId);
  }

  /**
   * Persist job status.
   */
  private void setStatus(final JobStatus.State status, final String containerId) {
    state.setJobStatus(name, new JobStatus(descriptor, status, containerId));
    this.status = status;
  }

  /**
   * Get the current job status.
   */
  public JobStatus.State getStatus() {
    return status;
  }

  /**
   * Create docker container configuration for a job.
   */
  private ContainerConfig containerConfig(final JobDescriptor descriptor) {
    final ContainerConfig containerConfig = new ContainerConfig();
    containerConfig.setImage(descriptor.getImage());
    final List<String> command = descriptor.getCommand();
    containerConfig.setCmd(command.toArray(new String[command.size()]));
    return containerConfig;
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

    public RunnerImpl(final long delayMillis) {
      this.delayMillis = delayMillis;
      executor.execute(this);
    }

    @Override
    public ListenableFuture<Integer> result() {
      return this;
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public void run() {
      try {
        // Delay
        Thread.sleep(delayMillis);

        // Get centrally registered status
        final JobStatus jobStatus = state.getJobStatus(name);
        final String registeredContainerId = (jobStatus == null) ? null : jobStatus.getId();

        // Check if container exists
        ContainerInspectResponse containerInfo = null;
        if (registeredContainerId != null) {
          log.info("inspecting container: {}: {}", descriptor, registeredContainerId);
          try {
            containerInfo = docker.inspectContainer(registeredContainerId).get();
          } catch (ExecutionException e) {
            // A round about way of saying that if it's a DockerException because the container
            // doesn't exist, that's ok, but otherwise, something went bad.
            if (e.getCause().getClass() != DockerException.class
                || !e.getMessage().contains("No such container")) {
              throw e;
            }
          }
        }
        // Check if the image exists
        final String image = descriptor.getImage();
        final List<Image> images = docker.getImages(image).get();
        if (images.isEmpty()) {
          final ClientResponse pull = docker.pull(image).get();
          // Wait until image is completely pulled
          pullStream = pull.getEntityInputStream();
          jsonTail("pull " + image, pullStream);
        }

        // Create and start container if necessary
        final String containerId;
        if (containerInfo != null && containerInfo.state.running) {
          containerId = registeredContainerId;
        } else {
          setStatus(CREATING, null);
          final ContainerConfig containerConfig = containerConfig(descriptor);
          final ContainerCreateResponse container = docker.createContainer(containerConfig).get();
          containerId = container.id;
          log.info("created container: {}: {}", descriptor, container);

          setStatus(STARTING, containerId);
          log.info("starting container: {}: {}", descriptor, containerId);
          startFuture = docker.startContainer(containerId);
          startFuture.get();
          log.info("started container: {}: {}: {}", descriptor, containerId, containerInfo);
        }

        setStatus(RUNNING, containerId);

        // Wait for container to die
        final int exitCode = docker.waitContainer(containerId).get();
        log.info("container exited: {}: {}: {}", descriptor, containerId, exitCode);
        setStatus(EXITED, containerId);

        set(exitCode);
      } catch (InterruptedException e) {
        setException(e);
      } catch (Exception e) {
        // Keep separate catch clauses to simplify setting breakpoints on actual errors
        setException(e);
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

    private String name;
    private JobDescriptor descriptor;
    private State state;
    private AsyncDockerClient dockerClient;
    private long restartIntervalMillis = DEFAULT_RESTART_INTERVAL_MILLIS;
    private long retryIntervalMillis = DEFAULT_RETRY_INTERVAL_MILLIS;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setDescriptor(final JobDescriptor descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    public Builder setState(final State state) {
      this.state = state;
      return this;
    }

    public Builder setDockerClient(final AsyncDockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    public Builder setRestartIntervalMillis(final long restartIntervalMillis) {
      this.restartIntervalMillis = restartIntervalMillis;
      return this;
    }

    public Builder setRetryIntervalMillis(final long retryIntervalMillis) {
      this.retryIntervalMillis = retryIntervalMillis;
      return this;
    }

    public Supervisor build() {
      return new Supervisor(name, descriptor, state, dockerClient, restartIntervalMillis,
                            retryIntervalMillis);
    }
  }
}
