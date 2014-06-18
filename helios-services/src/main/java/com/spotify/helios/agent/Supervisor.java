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

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Supervises docker containers for a single job.
 */
public class Supervisor {

  public interface Listener {

    void stateChanged(Supervisor supervisor);
  }

  private static final Logger log = LoggerFactory.getLogger(Supervisor.class);

  private final DockerClient docker;
  private final Job job;
  private final RestartPolicy restartPolicy;
  private final SupervisorMetrics metrics;
  private final Reactor reactor;
  private final Listener listener;
  private final TaskRunnerFactory runnerFactory;
  private final StatusUpdater statusUpdater;
  private final TaskMonitor monitor;

  private volatile Goal goal;
  private volatile String containerId;
  private volatile TaskRunner runner;
  private volatile Command currentCommand;
  private volatile Command performedCommand;

  public Supervisor(final Builder builder) {
    this.job = checkNotNull(builder.job, "job");
    this.docker = checkNotNull(builder.dockerClient, "docker");
    this.restartPolicy = checkNotNull(builder.restartPolicy, "restartPolicy");
    this.metrics = checkNotNull(builder.metrics, "metrics");
    this.listener = checkNotNull(builder.listener, "listener");
    this.currentCommand = new Nop();
    this.containerId = builder.existingContainerId;
    this.runnerFactory = checkNotNull(builder.runnerFactory, "runnerFactory");
    this.statusUpdater = checkNotNull(builder.statusUpdater, "statusUpdater");
    this.monitor = checkNotNull(builder.monitor, "monitor");
    this.reactor = new DefaultReactor("supervisor-" + job.getId(), new Update(),
                                      SECONDS.toMillis(30));
    this.reactor.startAsync();
    statusUpdater.setContainerId(containerId);
  }

  public void setGoal(final Goal goal) {
    if (this.goal == goal) {
      return;
    }
    log.debug("Supervisor {}: setting goal: {}", job.getId(), goal);
    this.goal = goal;
    statusUpdater.setGoal(goal);
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
    private String existingContainerId;
    private DockerClient dockerClient;
    private RestartPolicy restartPolicy;
    private SupervisorMetrics metrics;
    private Listener listener = new NopListener();
    private TaskRunnerFactory runnerFactory;
    private StatusUpdater statusUpdater;
    private TaskMonitor monitor;


    public Builder setJob(final Job job) {
      this.job = job;
      return this;
    }

    public Builder setExistingContainerId(final String existingContainerId) {
      this.existingContainerId = existingContainerId;
      return this;
    }

    public Builder setRestartPolicy(final RestartPolicy restartPolicy) {
      this.restartPolicy = restartPolicy;
      return this;
    }

    public Builder setDockerClient(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
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

    public Builder setStatusUpdater(final StatusUpdater statusUpdater) {
      this.statusUpdater = statusUpdater;
      return this;
    }

    public Builder setMonitor(final TaskMonitor monitor) {
      this.monitor = monitor;
      return this;
    }

    public Supervisor build() {
      return new Supervisor(this);
    }

    private class NopListener implements Listener {

      @Override
      public void stateChanged(final Supervisor supervisor) {

      }
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

      // Check if the runner exited normally or threw an exception
      final Result<Integer> result = runner.result();
      if (!result.isSuccess()) {
        // Runner threw an exception, inspect it.
        final Throwable t = result.getException();
        if (t instanceof InterruptedException || t instanceof InterruptedIOException) {
          // We're probably shutting down, remove the runner and bail.
          log.debug("task runner interrupted");
          runner = null;
          reactor.signal();
          return;
        }
        log.error("task runner threw exception", t);
      }

      // Restart the task
      startAfter(restartPolicy.delay(monitor.throttle()));
    }

    private void startAfter(final long delay) {
      log.debug("starting job (delay={}): {}", delay, job);
      runner = runnerFactory.create(delay, containerId, new TaskListener());
      runner.startAsync();
      runner.resultFuture().addListener(reactor.signalRunnable(), sameThreadExecutor());
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
        runner.stopAsync().awaitTerminated();
        runner = null;
      }

      // Kill the container after stopping the runner
      while (!containerNotRunning()) {
        killContainer();
        Thread.sleep(retryScheduler.nextMillis());
      }

      statusUpdater.setState(STOPPED);
      statusUpdater.update();
    }

    private void killContainer() throws InterruptedException {
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

  private class TaskListener extends TaskRunner.NopListener {

    @Override
    public void failed(final Throwable t) {
      metrics.containersThrewException();
    }

    @Override
    public void pulling() {
      metrics.containerPull();
    }

    @Override
    public void created(final String createdContainerId) {
      containerId = createdContainerId;
    }
  }
}
