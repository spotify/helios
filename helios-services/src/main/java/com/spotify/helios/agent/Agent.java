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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static com.spotify.helios.servicescommon.Reactor.Callback;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs jobs to implement the desired container deployment state.
 */
public class Agent extends AbstractIdleService {

  public static final Map<JobId, Execution> EMPTY_EXECUTIONS = Collections.emptyMap();

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private static final long UPDATE_INTERVAL = SECONDS.toMillis(30);

  private static final Predicate<Execution> PORT_ALLOCATION_PENDING = new Predicate<Execution>() {
    @Override
    public boolean apply(final Execution execution) {
      assert execution != null;
      return execution.getGoal() != UNDEPLOY && execution.getPorts() == null;
    }
  };

  private final AgentModel model;
  private final SupervisorFactory supervisorFactory;
  private final ModelListener modelListener = new ModelListener();
  private final Supervisor.Listener supervisorListener = new SupervisorListener();
  private final Map<JobId, Supervisor> supervisors = Maps.newHashMap();
  private final Reactor reactor;
  private final PersistentAtomicReference<Map<JobId, Execution>> executions;
  private final PortAllocator portAllocator;

  /**
   * Create a new agent.
   *
   * @param model             The model.
   * @param supervisorFactory The factory to use for creating supervisors.
   * @param reactorFactory    The factory to use for creating reactors.
   * @param executions        A persistent map of executions.
   * @param portAllocator     Allocator for job ports.
   */
  public Agent(final AgentModel model, final SupervisorFactory supervisorFactory,
               final ReactorFactory reactorFactory,
               final PersistentAtomicReference<Map<JobId, Execution>> executions,
               final PortAllocator portAllocator) {
    this.model = model;
    this.supervisorFactory = supervisorFactory;
    this.executions = executions;
    this.portAllocator = portAllocator;
    this.reactor = reactorFactory.create("agent", new Update(), UPDATE_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    for (final Entry<JobId, Execution> entry : executions.get().entrySet()) {
      final Execution execution = entry.getValue();
      final Job job = execution.getJob();
      if (execution.getPorts() != null) {
        createSupervisor(job, execution.getPorts());
      }
    }
    model.addListener(modelListener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
    for (final Supervisor supervisor : supervisors.values()) {
      supervisor.close();
      supervisor.join();
    }
  }

  /**
   * Create a job supervisor.
   *
   * @param job The job .
   */
  private Supervisor createSupervisor(final Job job, final Map<String, Integer> portAllocation) {
    log.debug("creating job supervisor: {}", job);
    final TaskStatus taskStatus = model.getTaskStatus(job.getId());
    final String containerId = (taskStatus == null) ? null : taskStatus.getContainerId();
    final Supervisor supervisor = supervisorFactory.create(job, containerId, portAllocation,
                                                           supervisorListener);
    supervisors.put(job.getId(), supervisor);
    return supervisor;
  }

  /**
   * Listens to model state updates and signals the reactor.
   */
  private class ModelListener implements AgentModel.Listener {

    @Override
    public void tasksChanged(final AgentModel model) {
      reactor.signal();
    }
  }

  /**
   * Listens to supervisor state updates and signals the reactor.
   */
  private class SupervisorListener implements Supervisor.Listener {

    @Override
    public void stateChanged(final Supervisor supervisor) {
      reactor.signal();
    }
  }

  /**
   * Starts and stops supervisors to reflect the desired state. Called by the reactor.
   */
  private class Update implements Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {

      // Note: when changing this code:
      // * Ensure that supervisors for the same container never run concurrently.
      // * A supervisor must not be released before its container is stopped.
      // * A new container must either reuse an existing supervisor or wait for the old supervisor
      //   to die before spawning a new one.
      // * Book-keeping a supervisor of one job should not block processing of other jobs

      final Map<JobId, Task> tasks = model.getTasks();

      log.debug("tasks: {}", tasks);
      log.debug("executions: {}", executions.get());
      log.debug("supervisors: {}", supervisors);

      // Create and update executions
      final Map<JobId, Execution> newExecutions = Maps.newHashMap(executions.get());
      for (Entry<JobId, Task> entry : tasks.entrySet()) {
        final JobId jobId = entry.getKey();
        final Task task = entry.getValue();
        final Execution existing = newExecutions.get(jobId);
        if (existing != null) {
          if (existing.getGoal() != task.getGoal()) {
            final Execution execution = existing.withGoal(task.getGoal());
            newExecutions.put(jobId, execution);
          }
        } else  {
          newExecutions.put(jobId, Execution.of(task.getJob()).withGoal(task.getGoal()));
        }
      }

      // Allocate ports
      final Map<JobId, Execution> pending = ImmutableMap.copyOf(
          Maps.filterValues(newExecutions, PORT_ALLOCATION_PENDING));
      if (!pending.isEmpty()) {
        final ImmutableSet.Builder<Integer> usedPorts = ImmutableSet.builder();
        final Map<JobId, Execution> allocated = Maps.filterKeys(newExecutions,
                                                                not(in(pending.keySet())));
        for (final Entry<JobId, Execution> entry : allocated.entrySet()) {
          usedPorts.addAll(entry.getValue().getPorts().values());
        }

        for (final Entry<JobId, Execution> entry : pending.entrySet()) {
          final JobId jobId = entry.getKey();
          final Execution execution = entry.getValue();
          final Job job = execution.getJob();
          final Map<String, Integer> ports = portAllocator.allocate(job.getPorts(),
                                                                    usedPorts.build());
          log.debug("Allocated ports for job {}: {}", jobId, ports);
          if (ports != null) {
            newExecutions.put(jobId, execution.withPorts(ports));
            usedPorts.addAll(ports.values());
          } else {
            log.warn("Unable to allocate ports for job: {}", job);
          }
        }
      }

      // Persist executions
      if (!newExecutions.equals(executions.get())) {
        executions.setUnchecked(ImmutableMap.copyOf(newExecutions));
      }

      // Remove stopped supervisors.
      for (final Entry<JobId, Supervisor> entry : ImmutableSet.copyOf(supervisors.entrySet())) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = entry.getValue();
        if (supervisor.isStopping() && supervisor.isDone()) {
          log.debug("releasing stopped supervisor: {}", jobId);
          supervisors.remove(jobId);
          supervisor.close();
          reactor.signal();
        }
      }

      // Create new supervisors
      for (final Entry<JobId, Execution> entry : executions.get().entrySet()) {
        final JobId jobId = entry.getKey();
        final Execution execution = entry.getValue();
        final Supervisor supervisor = supervisors.get(jobId);
        if (supervisor == null &&
            execution.getGoal() == START &&
            execution.getPorts() != null) {
          createSupervisor(execution.getJob(), execution.getPorts());
        }
      }

      // Update supervisor goals
      for (final Map.Entry<JobId, Supervisor> entry : supervisors.entrySet()) {
        final JobId jobId = entry.getKey();
        final Supervisor supervisor = entry.getValue();
        final Execution execution = executions.get().get(jobId);
        supervisor.setGoal(execution.getGoal());
      }

      // Reap dead executions
      final Set<JobId> reapedTasks = Sets.newHashSet();
      for (Entry<JobId, Execution> entry : executions.get().entrySet()) {
        final JobId jobId = entry.getKey();
        final Execution execution = entry.getValue();
        if (execution.getGoal() == UNDEPLOY) {
          final Supervisor supervisor = supervisors.get(jobId);
          if (supervisor == null) {
            reapedTasks.add(jobId);
            log.debug("Removing tombstoned task: {}", jobId);
            model.removeUndeployTombstone(jobId);
            model.removeTaskStatus(jobId);
          }
        }
      }

      // Persist executions
      if (!reapedTasks.isEmpty()) {
        final Map<JobId, Execution> survivors = Maps.filterKeys(executions.get(),
                                                                not(in(reapedTasks)));
        executions.setUnchecked(ImmutableMap.copyOf(survivors));
      }
    }
  }
}
