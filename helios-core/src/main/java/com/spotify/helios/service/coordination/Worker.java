/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.coordination;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.service.descriptors.AgentJobDescriptor;
import com.spotify.helios.service.descriptors.JobGoal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.difference;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Worker {

  private static final Logger log = LoggerFactory.getLogger(Worker.class);

  private final State state;
  private final JobRunnerFactory jobRunnerFactory;

  private final Reactor reactor;

  private final StateListener stateListener = new StateListener();
  private final Map<String, JobRunner> jobs = Maps.newHashMap();

  /**
   * Create a new worker.
   *
   * @param state            The desired state.
   * @param jobRunnerFactory The factory to use for creating jobs.
   */
  public Worker(final State state, final JobRunnerFactory jobRunnerFactory) {
    this.reactor = new Reactor(new Update());
    this.state = state;
    this.jobRunnerFactory = jobRunnerFactory;
    this.state.addListener(stateListener);
  }

  @VisibleForTesting
  Worker(final Reactor reactor, final State state, final JobRunnerFactory jobRunnerFactory) {
    this.reactor = reactor;
    this.state = state;
    this.jobRunnerFactory = jobRunnerFactory;
    this.state.addListener(stateListener);
  }

  /**
   * Stop this worker.
   */
  public void close() {
    reactor.close();
    this.state.removeListener(stateListener);
    for (final Map.Entry<String, JobRunner> entry : jobs.entrySet()) {
      entry.getValue().close();
    }
  }

  /**
   * Start a job.
   *
   * @param name       The job name.
   * @param descriptor The job descriptor.
   */
  private void startJobRunner(final String name, final AgentJobDescriptor descriptor) {
    log.debug("starting job: name={}, descriptor={}", name, descriptor);
    final JobRunner jobRunner = jobRunnerFactory.create(name, descriptor.getDescriptor());
    jobs.put(name, jobRunner);
    jobRunner.start(descriptor.getJob().getGoal());
  }

  /**
   * Stop a job and remove its runner.
   *
   * @param name The job name.
   */
  private void removeJob(final String name) {
    final JobRunner jobRunner = jobs.remove(name);
    jobRunner.stop();
  }

  /**
   * Starts and stops jobs to reflect the desired state. Called by the reactor.
   */
  @VisibleForTesting void checkChanges() {
    // Get a snapshot of the desired state
    final Map<String, AgentJobDescriptor> desiredJobs = state.getJobs();
    final Set<String> desiredJobIds = desiredJobs.keySet();

    // Get a snapshot of the current state
    final Set<String> currentJobIds = Sets.newHashSet(jobs.keySet());

    // Stop removed jobs
    // current - desired == running that shouldn't run
    for (final String jobId : difference(currentJobIds, desiredJobIds)) {
      removeJob(jobId);
    }

    // Start new jobs
    // desired - current == not running that should run
    for (final String jobId : difference(desiredJobIds, currentJobIds)) {
      final AgentJobDescriptor jobDescriptor = desiredJobs.get(jobId);
      startJobRunner(jobId, jobDescriptor);
    }

    // Update job goals
    for (final Map.Entry<String, AgentJobDescriptor> entry : desiredJobs.entrySet()) {
      final String jobId = entry.getKey();
      final JobRunner runner = jobs.get(jobId);
      final JobGoal goal = entry.getValue().getJob().getGoal();
      runner.setGoal(goal);
    }
  }

  /**
   * Listens to desired state updates.
   */
  private class StateListener implements State.Listener {

    @Override
    public void containersUpdated(final State state) {
      reactor.update();
    }
  }

  private class Update implements Runnable {

    @Override
    public void run() {
      checkChanges();
    }
  }
}
