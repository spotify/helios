/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.Maps;

import com.spotify.helios.common.Reactor;
import com.spotify.helios.common.ReactorFactory;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.AgentJobDescriptor;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobGoal;
import com.spotify.helios.common.descriptors.JobStatus;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;

import static com.spotify.helios.common.descriptors.JobGoal.START;
import static com.spotify.helios.common.descriptors.JobGoal.STOP;
import static com.spotify.helios.common.descriptors.JobStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AgentTest {

  @Mock State state;
  @Mock SupervisorFactory supervisorFactory;
  @Mock ReactorFactory reactorFactory;

  @Mock Supervisor fooSupervisor;
  @Mock Supervisor barSupervisor;
  @Mock Reactor reactor;

  @Captor ArgumentCaptor<Runnable> callbackCaptor;
  @Captor ArgumentCaptor<State.Listener> listenerCaptor;
  @Captor ArgumentCaptor<Long> timeoutCaptor;

  final Map<String, AgentJobDescriptor> jobs = Maps.newHashMap();
  final Map<String, AgentJobDescriptor> unmodifiableJobs = Collections.unmodifiableMap(jobs);

  final Map<String, JobStatus> jobStatuses = Maps.newHashMap();
  final Map<String, JobStatus> unmodifiableJobStatuses = Collections.unmodifiableMap(jobStatuses);

  Agent sut;
  Runnable callback;
  State.Listener listener;

  static final String FOO_JOB = "foojob";
  static final JobDescriptor FOO_DESCRIPTOR = JobDescriptor.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .build();

  static final String BAR_JOB = "barjob";
  static final JobDescriptor BAR_DESCRIPTOR = JobDescriptor.newBuilder()
      .setCommand(asList("bar", "bar"))
      .setImage("bar:5656")
      .setName("bar")
      .setVersion("63")
      .build();

  @Before
  public void setup() {
    when(supervisorFactory.create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR))
        .thenReturn(fooSupervisor);
    when(supervisorFactory.create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR))
        .thenReturn(barSupervisor);
    when(reactorFactory.create(callbackCaptor.capture(), timeoutCaptor.capture()))
        .thenReturn(reactor);
    when(state.getJobs()).thenReturn(unmodifiableJobs);
    when(state.getJobStatuses()).thenReturn(unmodifiableJobStatuses);
    sut = new Agent(state, supervisorFactory, reactorFactory);
    verify(reactorFactory).create(any(Runnable.class), anyLong());
    callback = callbackCaptor.getValue();
  }

  private void startAgent() {
    sut.start();
    verify(state).addListener(listenerCaptor.capture());
    listener = listenerCaptor.getValue();
  }

  private void configure(final JobDescriptor descriptor, final JobGoal goal) {
    final AgentJob agentJob = new AgentJob(descriptor.getId(), goal);
    final AgentJobDescriptor agentJobDescriptor = new AgentJobDescriptor(agentJob, descriptor);
    jobs.put(descriptor.getId(), agentJobDescriptor);
  }

  private void start(JobDescriptor descriptor) {
    configure(descriptor, START);
    callback.run();
  }

  private void stop(JobDescriptor descriptor) {
    jobs.remove(descriptor.getId());
    callback.run();
  }

  @Test
  public void verifyReactorIsUpdatedWhenListenerIsCalled() {
    startAgent();
    listener.jobsUpdated(state);
    verify(reactor).update();
  }

  @Test
  public void verifyAgentRecoversState() {
    configure(FOO_DESCRIPTOR, START);
    configure(BAR_DESCRIPTOR, STOP);

    jobStatuses.put(FOO_DESCRIPTOR.getId(), new JobStatus(FOO_DESCRIPTOR, CREATING, "foo-container-1"));
    jobStatuses.put(BAR_DESCRIPTOR.getId(), new JobStatus(BAR_DESCRIPTOR, RUNNING, "bar-container-1"));

    when(fooSupervisor.isStarting()).thenReturn(false);
    when(barSupervisor.isStarting()).thenReturn(true);

    startAgent();

    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);
    verify(fooSupervisor).start();
    when(fooSupervisor.isStarting()).thenReturn(true);

    verify(supervisorFactory).create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR);
    verify(barSupervisor).stop();
    when(barSupervisor.isStarting()).thenReturn(false);

    callback.run();

    verify(fooSupervisor, times(1)).start();
    verify(barSupervisor, times(1)).stop();
  }

  @Test
  public void verifyAgentRecoversStateAndStopsUndesiredSupervisors() {
    jobStatuses.put(FOO_DESCRIPTOR.getId(),
                    new JobStatus(FOO_DESCRIPTOR, CREATING, "foo-container-1"));

    startAgent();

    // Verify that the undesired supervisor was created and then stopped
    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);
    verify(fooSupervisor).stop();
  }

  @Test
  public void verifyAgentStartsSupervisors() {
    startAgent();

    start(FOO_DESCRIPTOR);
    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);
    verify(fooSupervisor).start();
    when(fooSupervisor.isStarting()).thenReturn(true);

    start(BAR_DESCRIPTOR);
    verify(supervisorFactory).create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR);
    verify(barSupervisor).start();
    when(barSupervisor.isStarting()).thenReturn(true);

    callback.run();

    verify(fooSupervisor, times(1)).start();
    verify(barSupervisor, times(1)).start();
  }

  @Test
  public void verifyAgentStopsAndRecreatesSupervisors() {
    startAgent();

    // Verify that supervisor is stopped
    start(BAR_DESCRIPTOR);
    stop(BAR_DESCRIPTOR);
    verify(barSupervisor).stop();
    when(barSupervisor.getStatus()).thenReturn(STOPPED);

    // Verify that a new supervisor is created after the previous one is discarded
    callback.run();
    start(BAR_DESCRIPTOR);
    verify(supervisorFactory, times(2)).create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR);
    verify(barSupervisor, times(2)).start();
  }

  @Test
  public void verifyCloseDoesNotStopJobs() {
    startAgent();

    start(FOO_DESCRIPTOR);
    sut.close();
    verify(fooSupervisor).close();
    verify(fooSupervisor, never()).stop();
  }
}
