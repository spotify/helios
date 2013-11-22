/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.common.Reactor;
import com.spotify.helios.common.ReactorFactory;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Agent {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private final AgentModel model;
  private final SupervisorFactory supervisorFactory;

  private final Reactor reactor;

  private final ModelListener modelListener = new ModelListener();
  private final Map<JobId, Supervisor> supervisors = Maps.newHashMap();

  /**
   * Create a new worker.
   *
   * @param model             The model.
   * @param supervisorFactory The factory to use for creating supervisors.
   * @param reactorFactory    The factory to use for creating reactors.
   */
  public Agent(final AgentModel model, final SupervisorFactory supervisorFactory,
               final ReactorFactory reactorFactory) {
    this.reactor = reactorFactory.create(new Update(), SECONDS.toMillis(1));
    this.model = model;
    this.supervisorFactory = supervisorFactory;
  }

  public void start() {
    final Map<JobId, TaskStatus> jobStatuses = model.getTaskStatuses();
    final Map<JobId, Task> jobConfigurations = model.getTasks();
    for (final JobId jobId : jobStatuses.keySet()) {
      final TaskStatus taskStatus = jobStatuses.get(jobId);
      final Task config = jobConfigurations.get(jobId);
      final Supervisor supervisor = createSupervisor(jobId, taskStatus.getJob());
      delegate(supervisor, config);
    }
    this.model.addListener(modelListener);
  }

  /**
   * Stop this worker.
   */
  public void close() {
    reactor.close();
    this.model.removeListener(modelListener);
    for (final Map.Entry<JobId, Supervisor> entry : supervisors.entrySet()) {
      entry.getValue().close();
    }
  }

  /**
   * Create a job supervisor.
   *
   * @param jobId      The name of the job.
   * @param descriptor The job descriptor.
   */
  private Supervisor createSupervisor(final JobId jobId, final Job descriptor) {
    log.debug("creating job supervisor: name={}, descriptor={}", jobId, descriptor);
    final Supervisor supervisor = supervisorFactory.create(jobId, descriptor);
    supervisors.put(jobId, supervisor);
    return supervisor;
  }

  /**
   * Instructor supervisor to start or stop job depending on configuration.
   */
  private void delegate(final Supervisor supervisor, final Task task) {
    if (task == null) {
      supervisor.stop();
    } else {
      switch (task.getGoal()) {
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
  private class ModelListener implements AgentModel.Listener {

    @Override
    public void tasksChanged(final AgentModel model) {
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
      for (final JobId name : ImmutableSet.copyOf(supervisors.keySet())) {
        final Supervisor supervisor = supervisors.get(name);
        if (supervisor.getStatus() == STOPPED) {
          supervisors.remove(name);
        }
      }

      // Get a snapshot of the desired state
      final Map<JobId, Task> desiredJobs = model.getTasks();
      final Set<JobId> desiredJobIds = desiredJobs.keySet();

      // Get a snapshot of the current state
      final Set<JobId> currentJobIds = Sets.newHashSet(supervisors.keySet());

      // Stop removed supervisors
      // current - desired == running that shouldn't run
      for (final JobId jobId : difference(currentJobIds, desiredJobIds)) {
        final Supervisor supervisor = supervisors.get(jobId);
        supervisor.stop();
      }

      // Create new supervisors
      // desired - current == not running that should run
      for (final JobId jobId : difference(desiredJobIds, currentJobIds)) {
        final Task jobDescriptor = desiredJobs.get(jobId);
        createSupervisor(jobId, jobDescriptor.getJob());
      }

      // Update job goals
      for (final Map.Entry<JobId, Task> entry : desiredJobs.entrySet()) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = supervisors.get(jobId);
        delegate(supervisor, entry.getValue());
      }
    }
  }
}
