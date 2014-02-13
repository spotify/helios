/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AgentTest {

  @Mock private AgentModel model;
  @Mock private SupervisorFactory supervisorFactory;
  @Mock private ReactorFactory reactorFactory;

  @Mock private Supervisor fooSupervisor;
  @Mock private Supervisor barSupervisor;
  @Mock private Reactor reactor;

  @Captor private ArgumentCaptor<Reactor.Callback> callbackCaptor;
  @Captor private ArgumentCaptor<AgentModel.Listener> listenerCaptor;
  @Captor private ArgumentCaptor<Long> timeoutCaptor;

  private final Map<JobId, Task> jobs = Maps.newHashMap();
  private final Map<JobId, Task> unmodifiableJobs = Collections.unmodifiableMap(jobs);

  private final Map<JobId, TaskStatus> jobStatuses = Maps.newHashMap();
  private final Map<JobId, TaskStatus> unmodifiableJobStatuses = Collections.unmodifiableMap(jobStatuses);

  private Agent sut;
  private Reactor.Callback callback;
  private AgentModel.Listener listener;

  private static final Job FOO_DESCRIPTOR = Job.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .build();

  private static final Job BAR_DESCRIPTOR = Job.newBuilder()
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
    mockService(reactor);
    mockService(model);
    when(reactorFactory.create(anyString(), callbackCaptor.capture(), timeoutCaptor.capture()))
        .thenReturn(reactor);
    when(model.getTasks()).thenReturn(unmodifiableJobs);
    when(model.getTaskStatuses()).thenReturn(unmodifiableJobStatuses);
    sut = new Agent(model, supervisorFactory, reactorFactory);
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
    verify(reactor).update();
    callback.run();
  }

  private void configure(final Job job, final Goal goal) {
    final Task task = new Task(job, goal);
    jobs.put(job.getId(), task);
  }

  private void start(Job descriptor) throws InterruptedException {
    configure(descriptor, START);
    callback.run();
  }

  private void badStop(Job descriptor) throws InterruptedException {
    jobs.remove(descriptor.getId());
    callback.run();
  }

  private void stop(Job descriptor) throws InterruptedException {
    configure(descriptor, UNDEPLOY);
    callback.run();
  }

  @Test
  public void verifyReactorIsUpdatedWhenListenerIsCalled() throws Exception {
    startAgent();
    listener.tasksChanged(model);
    verify(reactor, times(2)).update();
  }

  @Test
  public void verifyAgentRecoversState() throws Exception {
    configure(FOO_DESCRIPTOR, START);
    configure(BAR_DESCRIPTOR, STOP);

    jobStatuses.put(FOO_DESCRIPTOR.getId(),
                    TaskStatus.newBuilder()
                        .setJob(FOO_DESCRIPTOR)
                        .setState(CREATING)
                        .setContainerId("foo-container-1")
                        .build());
    jobStatuses.put(BAR_DESCRIPTOR.getId(),
                    TaskStatus.newBuilder()
                        .setJob(BAR_DESCRIPTOR)
                        .setState(RUNNING)
                        .setContainerId("bar-container-1")
                        .build());

    when(fooSupervisor.isStarting()).thenReturn(false);
    when(fooSupervisor.isStopping()).thenReturn(false);
    when(fooSupervisor.isDone()).thenReturn(true);

    when(barSupervisor.isStarting()).thenReturn(true);
    when(barSupervisor.isStopping()).thenReturn(false);
    when(barSupervisor.isDone()).thenReturn(true);

    startAgent();

    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);
    verify(fooSupervisor).start();

    verify(supervisorFactory).create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR);
    verify(barSupervisor).stop();

    when(fooSupervisor.isStarting()).thenReturn(true);
    when(fooSupervisor.isStopping()).thenReturn(false);
    when(fooSupervisor.isDone()).thenReturn(true);

    when(barSupervisor.isStarting()).thenReturn(false);
    when(barSupervisor.isStopping()).thenReturn(true);
    when(barSupervisor.isDone()).thenReturn(true);

    verify(fooSupervisor, times(1)).start();
    verify(barSupervisor, times(1)).stop();
  }

  @Test
  public void verifyAgentRecoversStateAndStartsSupervisorsWithNoInstructions() throws Exception {
    jobStatuses.put(FOO_DESCRIPTOR.getId(),
                    TaskStatus.newBuilder()
                        .setJob(FOO_DESCRIPTOR)
                        .setState(CREATING)
                        .setContainerId("foo-container-1")
                        .build());

    startAgent();

    // Verify that the undesired supervisor was created and started
    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);

    // ... and then started
    verify(fooSupervisor).start();

    // And not stopped
    callback.run();
    verify(fooSupervisor, never()).stop();
  }

  @Test
  public void verifyAgentRecoversStateAndStopsUndesiredSupervisors() throws Exception {
    jobStatuses.put(FOO_DESCRIPTOR.getId(),
                    TaskStatus.newBuilder()
                        .setJob(FOO_DESCRIPTOR)
                        .setState(CREATING)
                        .setContainerId("foo-container-1")
                        .build());

    configure(FOO_DESCRIPTOR, UNDEPLOY);

    startAgent();

    // Verify that the undesired supervisor was created
    verify(supervisorFactory).create(FOO_DESCRIPTOR.getId(), FOO_DESCRIPTOR);

    // ... and then stopped
    verify(fooSupervisor).stop();

    // And not started again
    callback.run();
    verify(fooSupervisor, never()).start();
  }

  @Test
  public void verifyAgentStartsSupervisors() throws Exception {
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
  public void verifyAgentStopsAndRecreatesSupervisors() throws Exception {
    startAgent();

    // Verify that supervisor is started
    start(BAR_DESCRIPTOR);
    verify(barSupervisor).start();
    when(barSupervisor.isDone()).thenReturn(true);
    when(barSupervisor.isStopping()).thenReturn(false);
    when(barSupervisor.isStarting()).thenReturn(true);

    // Verify that removal of the job *doesn't* stop the supervisor
    badStop(BAR_DESCRIPTOR);
    // Stop should *not* have been called.
    verify(barSupervisor, never()).stop();

    // Stop it the correct way
    stop(BAR_DESCRIPTOR);
    verify(barSupervisor).stop();
    when(barSupervisor.getStatus()).thenReturn(STOPPED);
    when(barSupervisor.isDone()).thenReturn(true);
    when(barSupervisor.isStopping()).thenReturn(true);
    when(barSupervisor.isStarting()).thenReturn(false);

    // Verify that a new supervisor is created after the previous one is discarded
    callback.run();
    start(BAR_DESCRIPTOR);
    verify(supervisorFactory, times(2)).create(BAR_DESCRIPTOR.getId(), BAR_DESCRIPTOR);
    verify(barSupervisor, times(2)).start();
  }

  @Test
  public void verifyCloseDoesNotStopJobs() throws Exception {
    startAgent();

    start(FOO_DESCRIPTOR);
    sut.stopAsync().awaitTerminated();
    verify(fooSupervisor).close();
    verify(fooSupervisor, never()).stop();
  }
}
