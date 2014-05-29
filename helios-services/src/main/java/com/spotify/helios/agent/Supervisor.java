/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Collections.emptyMap;
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

  // TODO (dano): make these timeouts configurable
  private static final int DOCKER_REQUEST_TIMEOUT_SECONDS = 30;
  public static final int DOCKER_LONG_REQUEST_TIMEOUT_SECONDS = 60;

  private final MonitoredDockerClient docker;
  private final JobId jobId;
  private final Job job;
  private final AgentModel model;
  private final RestartPolicy restartPolicy;
  private final TaskStatusManager stateManager;
  private final SupervisorMetrics metrics;
  private final Reactor reactor;
  private final Listener listener;
  private final DefaultStatusUpdater statusUpdater;
  private final AtomicReference<Goal> goal = new AtomicReference<Goal>();
  private final AtomicReference<ThrottleState> throttle = new AtomicReference<ThrottleState>(
      ThrottleState.NO);
  private final TaskRunnerFactory runnerFactory;

  private volatile TaskRunner runner;
  private volatile Command currentCommand;
  private volatile Command performedCommand;

  private final Supplier<String> containerIdSupplier = new Supplier<String>() {
    @Override
    public String get() {
      final String containerId;
      final TaskStatus taskStatus = model.getTaskStatus(jobId);
      containerId = (taskStatus == null) ? null : taskStatus.getContainerId();
      return containerId;
    }
  };

  public Supervisor(final Builder builder) {
    this.jobId = checkNotNull(builder.jobId);
    this.job = checkNotNull(builder.job);
    this.model = checkNotNull(builder.model);
    this.docker = builder.docker;
    this.restartPolicy = checkNotNull(builder.restartPolicy);
    this.stateManager = checkNotNull(builder.stateManager);
    this.metrics = checkNotNull(builder.metrics);
    this.listener = builder.listener;
    this.currentCommand = new Nop();
    this.reactor = new DefaultReactor("supervisor-" + jobId, new Update(), SECONDS.toMillis(30));
    this.reactor.startAsync();
    this.statusUpdater = new DefaultStatusUpdater(goal, throttle, builder.containerUtil, stateManager,
        containerIdSupplier);
    this.runnerFactory = builder.runnerFactory;
  }

  public void setGoal(final Goal goal) {
    if (this.goal.get() == goal) {
      return;
    }
    log.debug("Supervisor {}: setting goal: {}", jobId, goal);
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
    return stateManager.getStatus();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final Command command = currentCommand;
      final boolean done = performedCommand == command;
      log.debug("Supervisor {}: update: performedCommand={}, command={}, done={}", jobId, performedCommand, command, done);
      command.perform(done);
      if (!done) {
        performedCommand = command;
        fireStateChanged();
      }
    }
  }

  private void fireStateChanged() {
    log.debug("Supervisor {}: state changed", jobId);
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
    private ServiceRegistrar registrar;
    private CommandWrapper commandWrapper;
    private String host;
    private SupervisorMetrics metrics;
    private RiemannFacade riemannFacade;
    private Listener listener;
    private TaskRunnerFactory runnerFactory;
    private ContainerUtil containerUtil;
    private MonitoredDockerClient docker;

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

    public Builder setServiceRegistrar(final @Nullable ServiceRegistrar registrar) {
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
      // TODO(drewc) these should be moved either to SupervisorFactory or elsewhere,
      // but *not* into the Supervisor constructor.
      this.docker = new MonitoredDockerClient(checkNotNull(dockerClient),
          metrics, riemannFacade, DOCKER_REQUEST_TIMEOUT_SECONDS,
          DOCKER_LONG_REQUEST_TIMEOUT_SECONDS, 120);
      this.containerUtil = new ContainerUtil(host, job, ports, envVars);
      this.runnerFactory = new TaskRunnerFactory(registrar, job, commandWrapper,
          containerUtil, metrics, docker, flapController);
      return new Supervisor(this);
    }
  }

  private interface Command {

    void perform(final boolean done) throws InterruptedException;
  }

  /**
   * Starts a container and attempts to keep it up indefinitely, restarting it when it exits.
   */
  private class Start implements Command {
    @Override
    public void perform(final boolean done) {
      if (runner == null) {
        // There's no active Runner, start it to bring up the container.
        startAfter(0);
        return;
      }

      if (runner.isRunning()) {
        // There's an active Runner, brought up by this or another Start command previously.
        return;
      }

      // TODO (dano): Fix Runner mechanism to ensure that the below cannot ever happen. Currently it is possible to by mistake (programming error) introduce an early return in the Runner that doesn't set the result.
      if (!runner.result().isDone()) {
        log.warn("runner not running but result future not done!");
        startAfter(restartPolicy.restartThrottle(throttle.get()));
        return;
      }

      // Get the value of the Runner result future
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
      log.debug("starting job: {} (delay={}): {}", jobId, delay, job);
      runner = runnerFactory.create(delay, containerIdSupplier, throttle, statusUpdater);
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

      log.debug("stopping job: id={}: job={}", jobId, job);

      final RetryScheduler retryScheduler = BoundedRandomExponentialBackoff.newBuilder()
          .setMinIntervalMillis(SECONDS.toMillis(1))
          .setMaxIntervalMillis(SECONDS.toMillis(30))
          .build().newScheduler();

      String containerId = containerIdSupplier.get();

      // Stop the runner
      if (runner != null) {
        // Gently tell the runner to stop
        runner.stopAsync();
        // Wait for runner to stop
        while (!awaitTerminated(runner, retryScheduler.nextMillis())) {
          // Kill the container to make the runner stop waiting for on it
          containerId = fromNullable(containerIdSupplier.get()).or(fromNullable(containerId)).orNull();
          killContainer(containerId);
          // Disrupt work in progress to speed the runner to it's demise
          if (runner != null) {
            runner.disrupt();
          }
        }
        runner = null;
      }

      // Kill the container after stopping the runner
      containerId = fromNullable(containerIdSupplier.get()).or(fromNullable(containerId)).orNull();
      while (!containerNotRunning(containerId)) {
        killContainer(containerId);
        Thread.sleep(retryScheduler.nextMillis());
      }

      statusUpdater.setStatus(STOPPED, containerId);
    }

    private boolean awaitTerminated(final TaskRunner runner, final long timeoutMillis) {
      try {
        runner.awaitTerminated(timeoutMillis, MILLISECONDS);
        return true;
      } catch (TimeoutException ignore) {
        return false;
      }
    }

    private void killContainer(final String containerId)
        throws InterruptedException {
      if (containerId == null) {
        return;
      }
      try {
        docker.kill(containerId);
      } catch (DockerException | InterruptedException e) {
        log.error("failed to kill container {}", containerId, e);
      }
    }

    private boolean containerNotRunning(final String containerId)
        throws InterruptedException {
      if (containerId == null) {
        return true;
      }
      final ContainerInspectResponse containerInfo;
      try {
        containerInfo = docker.safeInspectContainer(containerId);
      } catch (DockerException e) {
        log.error("failed to query container {}", containerId, e);
        return false;
      }
      return containerInfo == null || !containerInfo.state.running;
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
        .add("jobId", jobId)
        .add("job", job)
        .add("currentCommand", currentCommand)
        .add("performedCommand", performedCommand)
        .toString();
  }
}
