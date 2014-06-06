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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.spotify.helios.agent.Agent.EMPTY_EXECUTIONS;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AgentTest {

  public static final Set<Integer> EMPTY_PORT_SET = emptySet();
  @Mock private AgentModel model;
  @Mock private SupervisorFactory supervisorFactory;
  @Mock private ReactorFactory reactorFactory;

  @Mock private Supervisor fooSupervisor;
  @Mock private Supervisor barSupervisor;
  @Mock private Reactor reactor;
  @Mock private PortAllocator portAllocator;

  @Captor private ArgumentCaptor<Reactor.Callback> callbackCaptor;
  @Captor private ArgumentCaptor<AgentModel.Listener> listenerCaptor;
  @Captor private ArgumentCaptor<Long> timeoutCaptor;

  private static final Map<String, Integer> EMPTY_PORT_ALLOCATION = Collections.emptyMap();

  private final Map<JobId, Task> jobs = Maps.newHashMap();
  private final Map<JobId, Task> unmodifiableJobs = Collections.unmodifiableMap(jobs);

  private final Map<JobId, TaskStatus> jobStatuses = Maps.newHashMap();
  private final Map<JobId, TaskStatus> unmodifiableJobStatuses = Collections.unmodifiableMap(jobStatuses);

  private Agent sut;
  private Reactor.Callback callback;
  private AgentModel.Listener listener;
  private PersistentAtomicReference<Map<JobId, Execution>> executions;

  private static final Job FOO_DESCRIPTOR = Job.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .setPorts(ImmutableMap.of("p1", PortMapping.of(4711),
                                "p2", PortMapping.of(4712, 12345)))
      .build();

  private static final Map<String, Integer> FOO_PORT_ALLOCATION = ImmutableMap.of("p1", 30000,
                                                                                  "p2", 12345);
  private static final Set<Integer> FOO_PORT_SET =
      ImmutableSet.copyOf(FOO_PORT_ALLOCATION.values());

  private static final Job BAR_DESCRIPTOR = Job.newBuilder()
      .setCommand(asList("bar", "bar"))
      .setImage("bar:5656")
      .setName("bar")
      .setVersion("63")
      .build();

  private static final Map<String, Integer> BAR_PORT_ALLOCATION = ImmutableMap.of();


  @SuppressWarnings("unchecked")
  @Before
  public void setup() throws Exception {
    final Path executionsFile = Files.createTempFile("helios-agent-executions", ".json");
    executions = PersistentAtomicReference.create(executionsFile,
                                                  new TypeReference<Map<JobId, Execution>>() {},
                                                  Suppliers.ofInstance(EMPTY_EXECUTIONS));
    when(portAllocator.allocate(eq(FOO_DESCRIPTOR.getPorts()), anySet()))
        .thenReturn(FOO_PORT_ALLOCATION);
    when(portAllocator.allocate(eq(BAR_DESCRIPTOR.getPorts()), anySet()))
        .thenReturn(BAR_PORT_ALLOCATION);
    when(supervisorFactory.create(eq(FOO_DESCRIPTOR),
                                  anyMapOf(String.class, Integer.class),
                                  any(Supervisor.Listener.class)))
        .thenReturn(fooSupervisor);
    when(supervisorFactory.create(eq(BAR_DESCRIPTOR),
                                  anyMapOf(String.class, Integer.class),
                                  any(Supervisor.Listener.class)))
        .thenReturn(barSupervisor);
    mockService(reactor);
    when(reactorFactory.create(anyString(), callbackCaptor.capture(), timeoutCaptor.capture()))
        .thenReturn(reactor);
    when(model.getTasks()).thenReturn(unmodifiableJobs);
    when(model.getTaskStatuses()).thenReturn(unmodifiableJobStatuses);
    sut = new Agent(model, supervisorFactory, reactorFactory, executions, portAllocator);
  }

  private void mockService(final Service service) {
    when(service.stopAsync()).thenReturn(service);
    when(service.startAsync()).thenReturn(service);
  }

  private void startAgent() throws Exception {
    sut.startAsync().awaitRunning();
    verify(reactorFactory).create(anyString(), any(Reactor.Callback.class), anyLong());
    callback = callbackCaptor.getValue();
    verify(model).addListener(listenerCaptor.capture());
    listener = listenerCaptor.getValue();
    verify(reactor).signal();
  }

  private void configure(final Job job, final Goal goal) {
    final Task task = new Task(job, goal);
    jobs.put(job.getId(), task);
  }

  private void start(Job descriptor) throws InterruptedException {
    configure(descriptor, START);
    callback.run(false);
  }

  private void badStop(Job descriptor) throws InterruptedException {
    jobs.remove(descriptor.getId());
    callback.run(false);
  }

  private void stop(Job descriptor) throws InterruptedException {
    configure(descriptor, UNDEPLOY);
    callback.run(false);
  }

  @Test
  public void verifyReactorIsUpdatedWhenListenerIsCalled() throws Exception {
    startAgent();
    listener.tasksChanged(model);
    verify(reactor, times(2)).signal();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void verifyAgentRecoversState() throws Exception {
    configure(FOO_DESCRIPTOR, START);
    configure(BAR_DESCRIPTOR, STOP);

    final Map<JobId, Execution> newExecutions = Maps.newHashMap();

    newExecutions.put(FOO_DESCRIPTOR.getId(), Execution.of(FOO_DESCRIPTOR)
        .withGoal(START)
        .withPorts(EMPTY_PORT_ALLOCATION));

    newExecutions.put(BAR_DESCRIPTOR.getId(), Execution.of(BAR_DESCRIPTOR)
        .withGoal(START)
        .withPorts(EMPTY_PORT_ALLOCATION));

    executions.setUnchecked(newExecutions);

    startAgent();

    verify(portAllocator, never()).allocate(anyMap(), anySet());

    verify(supervisorFactory).create(eq(FOO_DESCRIPTOR),
                                     eq(EMPTY_PORT_ALLOCATION),
                                     any(Supervisor.Listener.class));
    verify(supervisorFactory).create(eq(BAR_DESCRIPTOR),
                                     eq(EMPTY_PORT_ALLOCATION),
                                     any(Supervisor.Listener.class));

    callback.run(false);

    verify(fooSupervisor).setGoal(START);
    verify(barSupervisor).setGoal(STOP);

    when(fooSupervisor.isStarting()).thenReturn(true);
    when(fooSupervisor.isStopping()).thenReturn(false);
    when(fooSupervisor.isDone()).thenReturn(true);

    when(barSupervisor.isStarting()).thenReturn(false);
    when(barSupervisor.isStopping()).thenReturn(true);
    when(barSupervisor.isDone()).thenReturn(true);

    callback.run(false);

    verify(fooSupervisor, atLeastOnce()).setGoal(START);
    verify(barSupervisor, atLeastOnce()).setGoal(STOP);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void verifyAgentRecoversStateAndStartsSupervisorsWithNoInstructions() throws Exception {

    final Map<JobId, Execution> newExecutions = Maps.newHashMap();

    newExecutions.put(FOO_DESCRIPTOR.getId(), Execution.of(FOO_DESCRIPTOR)
        .withGoal(START)
        .withPorts(EMPTY_PORT_ALLOCATION));

    executions.setUnchecked(newExecutions);

    startAgent();

    // Verify that the undesired supervisor was created and started
    verify(portAllocator, never()).allocate(anyMap(), anySet());
    verify(supervisorFactory).create(eq(FOO_DESCRIPTOR),
                                     eq(EMPTY_PORT_ALLOCATION), any(Supervisor.Listener.class));

    // ... and then started
    callback.run(false);
    verify(fooSupervisor).setGoal(START);

    when(fooSupervisor.isStarting()).thenReturn(true);
    when(fooSupervisor.isStopping()).thenReturn(false);
    when(fooSupervisor.isDone()).thenReturn(true);

    // And not stopped
    callback.run(false);
    verify(fooSupervisor, never()).setGoal(STOP);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void verifyAgentRecoversStateAndStopsUndesiredSupervisors() throws Exception {

    final Map<JobId, Execution> newExecutions = Maps.newHashMap();

    newExecutions.put(FOO_DESCRIPTOR.getId(), Execution.of(FOO_DESCRIPTOR)
        .withGoal(START)
        .withPorts(EMPTY_PORT_ALLOCATION));

    executions.setUnchecked(newExecutions);

    configure(FOO_DESCRIPTOR, UNDEPLOY);

    startAgent();

    // Verify that the undesired supervisor was created
    verify(portAllocator, never()).allocate(anyMap(), anySet());
    verify(supervisorFactory).create(eq(FOO_DESCRIPTOR),
                                     eq(EMPTY_PORT_ALLOCATION), any(Supervisor.Listener.class));

    // ... and then stopped
    callback.run(false);
    verify(fooSupervisor).setGoal(UNDEPLOY);

    when(fooSupervisor.isStopping()).thenReturn(true);
    when(fooSupervisor.isStarting()).thenReturn(false);
    when(fooSupervisor.isDone()).thenReturn(true);
    when(fooSupervisor.getStatus()).thenReturn(STOPPED);

    // And not started again
    callback.run(false);
    verify(fooSupervisor, never()).setGoal(START);
  }

  @Test
  public void verifyAgentStartsSupervisors() throws Exception {
    startAgent();

    start(FOO_DESCRIPTOR);
    verify(portAllocator).allocate(FOO_DESCRIPTOR.getPorts(), EMPTY_PORT_SET);
    verify(supervisorFactory).create(eq(FOO_DESCRIPTOR),
                                     eq(FOO_PORT_ALLOCATION),
                                     any(Supervisor.Listener.class));

    verify(fooSupervisor).setGoal(START);
    when(fooSupervisor.isStarting()).thenReturn(true);

    start(BAR_DESCRIPTOR);
    verify(portAllocator).allocate(BAR_DESCRIPTOR.getPorts(), FOO_PORT_SET);
    verify(supervisorFactory).create(eq(BAR_DESCRIPTOR),
                                     eq(EMPTY_PORT_ALLOCATION),
                                     any(Supervisor.Listener.class));
    verify(barSupervisor).setGoal(START);
    when(barSupervisor.isStarting()).thenReturn(true);

    callback.run(false);

    verify(fooSupervisor, atLeastOnce()).setGoal(START);
    verify(barSupervisor, atLeastOnce()).setGoal(START);
  }

  @Test
  public void verifyAgentStopsAndRecreatesSupervisors() throws Exception {
    startAgent();

    // Verify that supervisor is started
    start(FOO_DESCRIPTOR);
    verify(portAllocator).allocate(FOO_DESCRIPTOR.getPorts(), EMPTY_PORT_SET);
    verify(fooSupervisor).setGoal(START);
    when(fooSupervisor.isDone()).thenReturn(true);
    when(fooSupervisor.isStopping()).thenReturn(false);
    when(fooSupervisor.isStarting()).thenReturn(true);

    // Verify that removal of the job *doesn't* stop the supervisor
    badStop(FOO_DESCRIPTOR);
    // Stop should *not* have been called.
    verify(fooSupervisor, never()).setGoal(STOP);

    // Stop it the correct way
    stop(FOO_DESCRIPTOR);
    verify(fooSupervisor, atLeastOnce()).setGoal(UNDEPLOY);
    when(fooSupervisor.getStatus()).thenReturn(STOPPED);
    when(fooSupervisor.isDone()).thenReturn(true);
    when(fooSupervisor.isStopping()).thenReturn(true);
    when(fooSupervisor.isStarting()).thenReturn(false);
    callback.run(false);

    // Verify that a new supervisor is created after the previous one is discarded
    start(FOO_DESCRIPTOR);
    verify(portAllocator, times(2)).allocate(FOO_DESCRIPTOR.getPorts(), EMPTY_PORT_SET);
    verify(supervisorFactory, times(2)).create(eq(FOO_DESCRIPTOR),
                                               eq(FOO_PORT_ALLOCATION),
                                               any(Supervisor.Listener.class));
    verify(fooSupervisor, atLeast(2)).setGoal(START);
  }

  @Test
  public void verifyCloseDoesNotStopJobs() throws Exception {
    startAgent();

    start(FOO_DESCRIPTOR);
    sut.stopAsync().awaitTerminated();
    verify(fooSupervisor).close();
    verify(fooSupervisor).join();
    verify(fooSupervisor, never()).setGoal(STOP);
  }
}
