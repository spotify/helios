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

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Supervises docker containers for a single job.
 */
public class Supervisor {

  public interface Listener {

    void stateChanged(Supervisor supervisor);
  }

  private static final Logger log = LoggerFactory.getLogger(Supervisor.class);

  public static final ThreadFactory RUNNER_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-supervisor-runner-%d").build();

  private final DockerClient docker;
  private final Job job;
  private final AgentModel model;
  private final RestartPolicy restartPolicy;
  private final TaskStatusManager statusManager;
  private final SupervisorMetrics metrics;
  private final Reactor reactor;
  private final Listener listener;
  private final DefaultStatusUpdater statusUpdater;
  private final AtomicReference<Goal> goal = new AtomicReference<>();
  private final AtomicReference<ThrottleState> throttle = new AtomicReference<>(ThrottleState.NO);
  private final TaskRunnerFactory runnerFactory;

  private volatile TaskRunner runner;
  private volatile Command currentCommand;
  private volatile Command performedCommand;

  private final Supplier<String> containerIdSupplier = new Supplier<String>() {
    @Override
    public String get() {
      final TaskStatus taskStatus = model.getTaskStatus(job.getId());
      return (taskStatus == null) ? null : taskStatus.getContainerId();
    }
  };

  public Supervisor(final Builder builder) {
    this.job = checkNotNull(builder.job, "job");
    this.model = checkNotNull(builder.model, "model");
    this.docker = checkNotNull(builder.dockerClient, "docker");
    this.restartPolicy = checkNotNull(builder.restartPolicy, "restartPolicy");
    this.statusManager = checkNotNull(builder.stateManager, "statusManager");
    this.metrics = checkNotNull(builder.metrics, "metrics");
    this.listener = builder.listener;
    this.currentCommand = new Nop();
    this.reactor = new DefaultReactor("supervisor-" + job.getId(), new Update(),
                                      SECONDS.toMillis(30));
    this.reactor.startAsync();
    this.statusUpdater = new DefaultStatusUpdater(goal, throttle, builder.taskConfig,
                                                  statusManager, containerIdSupplier);
    this.runnerFactory = builder.runnerFactory;
  }

  public void setGoal(final Goal goal) {
    if (this.goal.get() == goal) {
      return;
    }
    log.debug("Supervisor {}: setting goal: {}", job.getId(), goal);
    this.goal.set(goal);
    switch (goal) {
      case START:
        currentCommand = new Start();
        reactor.signal();
        metrics.supervisorStarted();
        break;
      case STOP:
      case UNDEPLOY:
        currentCommand = new Stop();
        reactor.signal();
        metrics.supervisorStopped();
        break;
    }
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
      // Stop the runner again in case it was rewritten by the reactor before it terminated.
      runner.stopAsync();
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
   * Get the current job status.
   */
  public TaskStatus.State getStatus() {
    return statusManager.getStatus();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final Command command = currentCommand;
      final boolean done = performedCommand == command;
      log.debug("Supervisor {}: update: performedCommand={}, command={}, done={}",
                job.getId(), performedCommand, command, done);
      command.perform(done);
      if (!done) {
        performedCommand = command;
        fireStateChanged();
      }
    }
  }

  private void fireStateChanged() {
    log.debug("Supervisor {}: state changed", job.getId());
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

    private Builder() {
    }

    private Job job;
    private AgentModel model;
    private DockerClient dockerClient;
    private RestartPolicy restartPolicy;
    private TaskStatusManager stateManager;
    private SupervisorMetrics metrics;
    private Listener listener;
    private TaskRunnerFactory runnerFactory;
    private TaskConfig taskConfig;

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

    public Builder setDockerClient(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    public Builder setTaskStatusManager(final TaskStatusManager manager) {
      stateManager = manager;
      return this;
    }

    public Builder setMetrics(SupervisorMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder setListener(final Listener listener) {
      this.listener = listener;
      return this;
    }

    public Builder setRunnerFactory(final TaskRunnerFactory runnerFactory) {
      this.runnerFactory = runnerFactory;
      return this;
    }

    public Builder setTaskConfig(final TaskConfig taskConfig) {
      this.taskConfig = taskConfig;
      return this;
    }

    public Supervisor build() {
      return new Supervisor(this);
    }
  }

  private interface Command {

    /**
     * Perform the command. Although this is declared to throw InterruptedException, this will only
     * happen when the supervisor is being shut down. During normal operations, the operation will
     * be allowed to run until it's done.
     */
    void perform(final boolean done) throws InterruptedException;
  }

  /**
   * Starts a container and attempts to keep it up indefinitely, restarting it when it exits.
   */
  private class Start implements Command {

    @Override
    public void perform(final boolean done) throws InterruptedException {
      if (runner == null) {
        // There's no active runner, start it to bring up the container.
        startAfter(0);
        return;
      }

      if (runner.isRunning()) {
        // There's an active runner, brought up by this or another Start command previously.
        return;
      }

      // TODO (dano): Fix TaskRunner mechanism to ensure that the below cannot ever happen.
      // TODO (dano): Currently it is possible to by mistake (programming error) introduce an
      // TODO (dano): early return in the Runner that doesn't set the result.
      if (!runner.result().isDone()) {
        log.warn("runner not running but result future not done!");
        startAfter(restartPolicy.restartThrottle(throttle.get()));
        return;
      }

      // Get the value of the runner result future
      final Result<Integer> result = Result.of(runner.result());
      checkState(result.isDone(), "BUG: runner future is done but result(future) is not done.");

      // Check if the runner exited normally or threw an exception
      if (result.isSuccess()) {
        // Runner exited normally without an exception, indicating that the container exited,
        // so we restart the container.
        startAfter(restartPolicy.restartThrottle(throttle.get()));
      } else {
        // Runner threw an exception, inspect it.
        final Throwable t = result.getException();
        if (t instanceof InterruptedException || t instanceof InterruptedIOException) {
          // We're probably shutting down. Ignore the exception.
          log.debug("task runner interrupted");
        } else {
          // Report the runner failure
          statusUpdater.setStatus(FAILED);
          log.error("task runner threw exception", t);
        }
        long restartDelay = restartPolicy.getRetryIntervalMillis();
        long throttleDelay = restartPolicy.restartThrottle(throttle.get());
        startAfter(Math.max(restartDelay, throttleDelay));
      }
    }

    private void startAfter(final long delay) {
      log.debug("starting job (delay={}): {}", delay, job);
      runner = runnerFactory.create(delay, containerIdSupplier.get(), throttle, statusUpdater);
      runner.startAsync();
      runner.result().addListener(reactor.signalRunnable(), sameThreadExecutor());
    }
  }

  /**
   * Stops a container, making sure that the runner spawned by {@link Start} is stopped and the
   * container is not running.
   */
  private class Stop implements Command {

    @Override
    public void perform(final boolean done) throws InterruptedException {
      if (done) {
        return;
      }

      log.debug("stopping job: {}", job);

      final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
          .setMinIntervalMillis(SECONDS.toMillis(1))
          .setMaxIntervalMillis(SECONDS.toMillis(30))
          .build().newScheduler();

      // Stop the runner
      if (runner != null) {
        // Gently tell the runner to stop
        runner.stopAsync();
        // Wait for runner to stop
        while (!awaitTerminated(runner, retryScheduler.nextMillis())) {
          // Kill the container to make the runner stop waiting for on it
          killContainer();
        }
        runner = null;
      }

      // Kill the container after stopping the runner
      while (!containerNotRunning()) {
        killContainer();
        Thread.sleep(retryScheduler.nextMillis());
      }

      statusUpdater.setStatus(STOPPED);
    }

    private boolean awaitTerminated(final TaskRunner runner, final long timeoutMillis) {
      try {
        runner.awaitTerminated(timeoutMillis, MILLISECONDS);
        return true;
      } catch (TimeoutException ignore) {
        return false;
      }
    }

    private void killContainer() throws InterruptedException {
      final String containerId = containerIdSupplier.get();
      if (containerId == null) {
        return;
      }
      try {
        docker.killContainer(containerId);
      } catch (DockerException e) {
        log.error("failed to kill container {}", containerId, e);
      }
    }

    private boolean containerNotRunning()
        throws InterruptedException {
      final String containerId = containerIdSupplier.get();
      if (containerId == null) {
        return true;
      }
      final ContainerInfo containerInfo;
      try {
        containerInfo = docker.inspectContainer(containerId);
      } catch (DockerException e) {
        log.error("failed to query container {}", containerId, e);
        return false;
      }
      return containerInfo == null || !containerInfo.state().running();
    }
  }

  private static class Nop implements Command {

    @Override
    public void perform(final boolean done) {
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("currentCommand", currentCommand)
        .add("performedCommand", performedCommand)
        .toString();
  }
}
