/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Predicate;
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
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Agent {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private final AgentModel model;
  private final SupervisorFactory supervisorFactory;
  private final ReactorFactory reactorFactory;

  private final ModelListener modelListener = new ModelListener();
  private final Map<JobId, Supervisor> supervisors = Maps.newHashMap();

  private Reactor reactor;

  /**
   * Create a new worker.
   *
   * @param model             The model.
   * @param supervisorFactory The factory to use for creating supervisors.
   * @param reactorFactory    The factory to use for creating reactors.
   */
  public Agent(final AgentModel model, final SupervisorFactory supervisorFactory,
               final ReactorFactory reactorFactory) {
    this.model = model;
    this.supervisorFactory = supervisorFactory;
    this.reactorFactory = reactorFactory;
  }

  public void start() {
    final Map<JobId, Task> tasks = model.getTasks();
    final Map<JobId, TaskStatus> tasksStatuses = model.getTaskStatuses();
    for (final Entry<JobId, TaskStatus> entry : tasksStatuses.entrySet()) {
      final JobId id = entry.getKey();
      final TaskStatus taskStatus = entry.getValue();
      final Task task = tasks.get(id);
      final Supervisor supervisor = createSupervisor(id, taskStatus.getJob());
      if (task == null) {
        supervisor.stop();
        model.removeTaskStatus(id);
      } else {
        delegate(supervisor, task, true);
      }
    }
    reactor = reactorFactory.create(new Update(), SECONDS.toMillis(1));
    model.addListener(modelListener);
  }

  /**
   * Stop this worker.
   */
  public void close() throws InterruptedException {
    reactor.close();
    this.model.removeListener(modelListener);
    model.close();
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
  private void delegate(final Supervisor supervisor, final Task task, final boolean boot) {
    switch (task.getGoal()) {
      case START:
        if (!supervisor.isStarting()) {
          supervisor.start();
        }
        break;
      case STOP:
        if (supervisor.isStarting() || boot) {
          supervisor.stop();
        }
        break;
      case UNDEPLOY:
        if (supervisor.isStarting() || boot) {
          supervisor.stop();
        }
        model.removeUndeployTombstone(task.getJob().getId());
        model.removeTaskStatus(task.getJob().getId());
        break;
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

      // Get a snapshot of currently configured tasks
      final Map<JobId, Task> tasks = model.getTasks();
      final Map<JobId, Task> activeTasks = Maps.filterEntries(
          tasks,
          new Predicate<Entry<JobId, Task>>() {
            @Override
            public boolean apply(Entry<JobId, Task> entry) {
              return entry.getValue().getGoal() != UNDEPLOY;
            }
          });
      // Compute the set we want to keep
      final Set<JobId> desiredJobIds = activeTasks.keySet();
      // The opposite is what we want to get rid of
      final Set<JobId> undesirableJobIds = difference(tasks.keySet(), desiredJobIds);

      // Get a snapshot of the current state
      final Set<JobId> currentJobIds = Sets.newHashSet(supervisors.keySet());

      // Stop tombstoned supervisors
      for (final JobId jobId : intersection(undesirableJobIds, currentJobIds)) {
        final Supervisor supervisor = supervisors.get(jobId);
        supervisor.stop();
        supervisors.remove(jobId);
        model.removeUndeployTombstone(jobId);
        model.removeTaskStatus(jobId);
      }

      // Create new supervisors
      // desired - current == not running that should run
      for (final JobId jobId : difference(desiredJobIds, currentJobIds)) {
        final Task jobDescriptor = activeTasks.get(jobId);
        createSupervisor(jobId, jobDescriptor.getJob());
      }

      // Update job goals
      for (final Map.Entry<JobId, Task> entry : activeTasks.entrySet()) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = supervisors.get(jobId);
        delegate(supervisor, entry.getValue(), false);
      }
    }
  }
}
