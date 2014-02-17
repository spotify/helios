/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Sets.difference;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static com.spotify.helios.servicescommon.Reactor.Callback;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Agent extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private static final Predicate<Entry<JobId, Task>> DEPLOYED_PRED =
      new Predicate<Entry<JobId, Task>>() {
        @Override
        public boolean apply(Entry<JobId, Task> entry) {
          return entry.getValue().getGoal() != UNDEPLOY;
        }
      };

  private static final Predicate<Entry<JobId, Task>> START_PRED =
      new Predicate<Entry<JobId, Task>>() {
        @Override
        public boolean apply(Entry<JobId, Task> entry) {
          return entry.getValue().getGoal() == START;
        }
      };

  public static final long UPDATE_INTERVAL = SECONDS.toMillis(30);

  private final AgentModel model;
  private final SupervisorFactory supervisorFactory;

  private final ModelListener modelListener = new ModelListener();
  private final Map<JobId, Supervisor> supervisors = Maps.newHashMap();

  private final Reactor reactor;

  /**
   * Create a new agent.
   *
   * @param model             The model.
   * @param supervisorFactory The factory to use for creating supervisors.
   * @param reactorFactory    The factory to use for creating reactors.
   */
  public Agent(final AgentModel model, final SupervisorFactory supervisorFactory,
               final ReactorFactory reactorFactory) {
    this.model = model;
    this.supervisorFactory = supervisorFactory;
    this.reactor = reactorFactory.create("agent", new Update(), UPDATE_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    final Map<JobId, TaskStatus> tasksStatuses = model.getTaskStatuses();
    for (final Entry<JobId, TaskStatus> entry : tasksStatuses.entrySet()) {
      final JobId id = entry.getKey();
      final TaskStatus taskStatus = entry.getValue();
      createSupervisor(id, taskStatus.getJob());
    }
    model.addListener(modelListener);
    reactor.startAsync().awaitRunning();
    reactor.update();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
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
   * Instruct supervisor to start or stop job depending on configuration.
   */
  private void delegate(final Supervisor supervisor, final Task task)
      throws InterruptedException {
    // TODO (dano): we should persist last intact set of instructions
    // In the absence of instructions, we run jobs until told otherwise.
    if (task == null) {
      if (!supervisor.isStarting()) {
        supervisor.start();
      }
      return;
    }

    switch (task.getGoal()) {
      case START:
        if (!supervisor.isStarting()) {
          supervisor.start();
        }
        break;
      case STOP:
      case UNDEPLOY:
        if (!supervisor.isStopping()) {
          supervisor.stop();
        }
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
  private class Update implements Callback {

    @Override
    public void run() throws InterruptedException {

      // Note: when changing this code:
      // * Ensure that supervisors for the same container never run concurrently.
      // * A supervisor must not be released before its container is stopped.
      // * A new container must either reuse an existing supervisor or wait for the old supervisor
      //   to die before spawning a new one.
      // * Book-keeping a supervisor of one job should not block processing of other jobs

      // Get a snapshot of currently configured tasks
      final Map<JobId, Task> tasks = model.getTasks();
      final Map<JobId, Task> deployedTasks = Maps.filterEntries(tasks, DEPLOYED_PRED);
      final Map<JobId, Task> startTasks = Maps.filterEntries(tasks, START_PRED);

      // Compute the set we want to keep
      final Set<JobId> deployedJobIds = deployedTasks.keySet();
      final Set<JobId> startJobIds = startTasks.keySet();

      // The opposite is what we want to get rid of
      final Set<JobId> undeployedJobIds = difference(tasks.keySet(), deployedJobIds);
      log.debug("tasks: {}", tasks.keySet());
      log.debug("deployed: {}", deployedJobIds);
      log.debug("undeployed: {}", undeployedJobIds);
      log.debug("start: {}", startJobIds);
      log.debug("current: {}", supervisors.keySet());

      // Remove stopped supervisors.
      // Note: We do not touch supervisors that we do not have instructions for.
      final Map<JobId, Supervisor> stopSupervisors = filterKeys(supervisors, in(undeployedJobIds));
      for (Entry<JobId, Supervisor> entry : stopSupervisors.entrySet()) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = entry.getValue();
        if (supervisor.isDone() && supervisor.getStatus() == STOPPED) {
          log.debug("releasing stopped supervisor: {}", jobId);
          supervisor.close();
          supervisors.remove(jobId);
        }
      }

      // Get a snapshot of the current state
      final Set<JobId> currentJobIds = Sets.newHashSet(supervisors.keySet());

      // Remove tombstoned tasks if the supervisor is gone
      for (final JobId jobId : undeployedJobIds) {
        final Supervisor supervisor = supervisors.get(jobId);
        if (supervisor == null) {
          log.debug("Removing tombstoned task: {}", jobId);
          model.removeUndeployTombstone(jobId);
          model.removeTaskStatus(jobId);
        }
      }

      // Create new supervisors
      // start - current == not running that should run
      for (final JobId jobId : difference(startJobIds, currentJobIds)) {
        final Task jobDescriptor = deployedTasks.get(jobId);
        createSupervisor(jobId, jobDescriptor.getJob());
      }

      // Update job goals
      for (final Map.Entry<JobId, Supervisor> entry : supervisors.entrySet()) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = entry.getValue();
        final Task task = tasks.get(jobId);
        delegate(supervisor, task);
      }
    }
  }
}
