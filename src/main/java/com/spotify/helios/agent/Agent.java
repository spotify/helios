/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.common.Reactor;
import com.spotify.helios.common.ReactorFactory;
import com.spotify.helios.common.descriptors.AgentJobDescriptor;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.spotify.helios.common.descriptors.JobStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Agent {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private final State state;
  private final SupervisorFactory supervisorFactory;

  private final Reactor reactor;

  private final StateListener stateListener = new StateListener();
  private final Map<String, Supervisor> supervisors = Maps.newHashMap();

  /**
   * Create a new worker.
   *
   * @param state             The desired state.
   * @param supervisorFactory The factory to use for creating supervisors.
   * @param reactorFactory    The factory to use for creating reactors.
   */
  public Agent(final State state, final SupervisorFactory supervisorFactory,
               final ReactorFactory reactorFactory) {
    this.reactor = reactorFactory.create(new Update(), SECONDS.toMillis(1));
    this.state = state;
    this.supervisorFactory = supervisorFactory;
  }

  public void start() {
    final Map<String, JobStatus> jobStatuses = state.getJobStatuses();
    final Map<String, AgentJobDescriptor> jobConfigurations = state.getJobs();
    for (final String name : jobStatuses.keySet()) {
      final JobStatus jobStatus = jobStatuses.get(name);
      final AgentJobDescriptor config = jobConfigurations.get(name);
      final Supervisor supervisor = createSupervisor(name, jobStatus.getJob());
      delegate(supervisor, config);
    }
    this.state.addListener(stateListener);
  }

  /**
   * Stop this worker.
   */
  public void close() {
    reactor.close();
    this.state.removeListener(stateListener);
    for (final Map.Entry<String, Supervisor> entry : supervisors.entrySet()) {
      entry.getValue().close();
    }
  }

  /**
   * Create a job supervisor.
   *
   * @param name       The name of the job.
   * @param descriptor The job descriptor.
   */
  private Supervisor createSupervisor(final String name, final JobDescriptor descriptor) {
    log.debug("creating job supervisor: name={}, descriptor={}", name, descriptor);
    final Supervisor supervisor = supervisorFactory.create(name, descriptor);
    supervisors.put(name, supervisor);
    return supervisor;
  }

  /**
   * Instructor supervisor to start or stop job depending on configuration.
   */
  private void delegate(final Supervisor supervisor, final AgentJobDescriptor config) {
    if (config == null) {
      supervisor.stop();
    } else {
      switch (config.getJob().getGoal()) {
        case START:
          if (!supervisor.isStarting()) {
            supervisor.start();
          }
          break;
        case STOP:
          if (supervisor.isStarting()) {
            supervisor.stop();
          }
          break;
      }
    }
  }

  /**
   * Listens to desired state updates.
   */
  private class StateListener implements State.Listener {

    @Override
    public void jobsUpdated(final State state) {
      reactor.update();
    }
  }

  /**
   * Starts and stops supervisors to reflect the desired state. Called by the reactor.
   */
  private class Update implements Runnable {

    @Override
    public void run() {
      // Remove stopped supervisors
      for (final String name : ImmutableSet.copyOf(supervisors.keySet())) {
        final Supervisor supervisor = supervisors.get(name);
        if (supervisor.getStatus() == STOPPED) {
          supervisors.remove(name);
        }
      }

      // Get a snapshot of the desired state
      final Map<String, AgentJobDescriptor> desiredJobs = state.getJobs();
      final Set<String> desiredJobIds = desiredJobs.keySet();

      // Get a snapshot of the current state
      final Set<String> currentJobIds = Sets.newHashSet(supervisors.keySet());

      // Stop removed supervisors
      // current - desired == running that shouldn't run
      for (final String jobId : difference(currentJobIds, desiredJobIds)) {
        final Supervisor supervisor = supervisors.get(jobId);
        supervisor.stop();
      }

      // Create new supervisors
      // desired - current == not running that should run
      for (final String jobId : difference(desiredJobIds, currentJobIds)) {
        final AgentJobDescriptor jobDescriptor = desiredJobs.get(jobId);
        createSupervisor(jobId, jobDescriptor.getDescriptor());
      }

      // Update job goals
      for (final Map.Entry<String, AgentJobDescriptor> entry : desiredJobs.entrySet()) {
        final String jobId = entry.getKey();
        final Supervisor supervisor = supervisors.get(jobId);
        delegate(supervisor, entry.getValue());
      }
    }
  }
}
