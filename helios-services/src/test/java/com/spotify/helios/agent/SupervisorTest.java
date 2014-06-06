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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SupervisorTest {

  final ExecutorService executor = Executors.newCachedThreadPool();

  static final String REPOSITORY = "spotify";
  static final String TAG = "17";
  static final String IMAGE = REPOSITORY + ":" + TAG;
  static final String NAME = "foobar";
  static final List<String> COMMAND = asList("foo", "bar");
  static final String VERSION = "4711";
  static final Job JOB = Job.newBuilder()
      .setName(NAME)
      .setCommand(COMMAND)
      .setImage(IMAGE)
      .setVersion(VERSION)
      .build();
  static final Map<String, String> ENV = ImmutableMap.of("foo", "17",
                                                         "bar", "4711");
  static final Set<String> EXPECTED_CONTAINER_ENV = ImmutableSet.of("foo=17", "bar=4711");

  public static final ContainerInfo RUNNING_RESPONSE = new ContainerInfo() {
    @Override
    public ContainerState state() {
      final ContainerState state = new ContainerState();
      state.running(true);
      return state;
    }

    @Override
    public NetworkSettings networkSettings() {
      return NetworkSettings.builder()
          .ports(Collections.<String, List<PortBinding>>emptyMap())
          .build();
    }
  };

  public static final ContainerInfo STOPPED_RESPONSE = new ContainerInfo() {
    @Override
    public ContainerState state() {
      final ContainerState state = new ContainerState();
      state.running(false);
      return state;
    }
  };

  @Mock public AgentModel model;
  @Mock public DockerClient docker;
  @Mock public RestartPolicy retryPolicy;

  @Captor public ArgumentCaptor<ContainerConfig> containerConfigCaptor;
  @Captor public ArgumentCaptor<String> containerNameCaptor;
  @Captor public ArgumentCaptor<TaskStatus> taskStatusCaptor;

  Supervisor sut;

  @Before
  public void setup() {
    when(retryPolicy.getRetryIntervalMillis()).thenReturn(10L);
    TaskStatusManagerImpl manager = TaskStatusManagerImpl.newBuilder()
        .setJob(JOB)
        .setModel(model)
        .build();
    final TaskConfig taskConfig = TaskConfig.builder()
        .host("AGENT_NAME")
        .job(JOB)
        .envVars(ENV)
        .build();
    final TaskRunnerFactory runnerFactory = TaskRunnerFactory.builder()
        .job(JOB)
        .taskConfig(taskConfig)
        .flapController(FlapController.newBuilder()
                            .setJobId(JOB.getId())
                            .setTaskStatusManager(manager)
                            .build())
        .dockerClient(docker)
        .build();
    sut = Supervisor.newBuilder()
        .setJob(JOB)
        .setModel(model)
        .setDockerClient(docker)
        .setRestartPolicy(retryPolicy)
        .setTaskConfig(taskConfig)
        .setTaskStatusManager(manager)
        .setRunnerFactory(runnerFactory)
        .setMetrics(new NoopSupervisorMetrics())
        .build();

    final ConcurrentMap<JobId, TaskStatus> statusMap = Maps.newConcurrentMap();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) {
        final Object[] arguments = invocationOnMock.getArguments();
        final JobId jobId = (JobId) arguments[0];
        final TaskStatus status = (TaskStatus) arguments[1];
        statusMap.put(jobId, status);
        return null;
      }
    }).when(model).setTaskStatus(eq(JOB.getId()), taskStatusCaptor.capture());
    when(model.getTaskStatus(eq(JOB.getId()))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final JobId jobId = (JobId) invocationOnMock.getArguments()[0];
        return statusMap.get(jobId);
      }
    });
  }

  @After
  public void teardown() throws Exception {
    if (sut != null) {
      sut.close();
      sut.join();
    }
  }

  @Test
  public void verifySupervisorStartsAndStopsDockerContainer() throws Exception {
    final String containerId = "deadbeef";

    final ContainerCreation createResponse = new ContainerCreation(containerId);

    final SettableFuture<ContainerCreation> createFuture = SettableFuture.create();
    when(docker.createContainer(any(ContainerConfig.class),
                                any(String.class))).thenAnswer(futureAnswer(createFuture));

    final SettableFuture<Void> startFuture = SettableFuture.create();
    doAnswer(futureAnswer(startFuture))
        .when(docker).startContainer(eq(containerId), any(HostConfig.class));

    final ImageInfo imageInfo = new ImageInfo();
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    final SettableFuture<ContainerExit> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenAnswer(futureAnswer(waitFuture));

    // Start the job
    sut.setGoal(START);

    // Verify that the container is created
    verify(docker, timeout(30000)).createContainer(containerConfigCaptor.capture(),
                                                   containerNameCaptor.capture());
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
                                                eq(TaskStatus.newBuilder()
                                                       .setJob(JOB)
                                                       .setGoal(START)
                                                       .setState(CREATING)
                                                       .setContainerId(null)
                                                       .setEnv(ENV)
                                                       .build())
    );
    assertEquals(CREATING, sut.getStatus());
    createFuture.set(createResponse);
    final ContainerConfig containerConfig = containerConfigCaptor.getValue();
    assertEquals(IMAGE, containerConfig.image());
    assertEquals(EXPECTED_CONTAINER_ENV, ImmutableSet.copyOf(containerConfig.env()));
    final String containerName = containerNameCaptor.getValue();

    assertEquals(JOB.getId().toShortString(), shortJobIdFromContainerName(containerName));

    // Verify that the container is started
    verify(docker, timeout(30000)).startContainer(eq(containerId), any(HostConfig.class));
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
                                                eq(TaskStatus.newBuilder()
                                                       .setJob(JOB)
                                                       .setGoal(START)
                                                       .setState(STARTING)
                                                       .setContainerId(containerId)
                                                       .setEnv(ENV)
                                                       .build())
    );
    assertEquals(STARTING, sut.getStatus());
    when(docker.inspectContainer(eq(containerId))).thenReturn(RUNNING_RESPONSE);
    startFuture.set(null);

    verify(docker, timeout(30000)).waitContainer(containerId);
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
                                                eq(TaskStatus.newBuilder()
                                                       .setJob(JOB)
                                                       .setGoal(START)
                                                       .setState(RUNNING)
                                                       .setContainerId(containerId)
                                                       .setEnv(ENV)
                                                       .build())
    );
    assertEquals(RUNNING, sut.getStatus());

    // Stop the job
    final SettableFuture<Void> killFuture = SettableFuture.create();
    doAnswer(futureAnswer(killFuture)).when(docker).killContainer(eq(containerId));
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO (dano): Make Supervisor.stop() asynchronous
        sut.setGoal(STOP);
        return null;
      }
    });
    verify(docker, timeout(30000)).killContainer(eq(containerId));

    // Change docker container state to stopped when it's killed
    when(docker.inspectContainer(eq(containerId))).thenReturn(STOPPED_RESPONSE);
    killFuture.set(null);

    // Verify that the stopped state is signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
                                                eq(TaskStatus.newBuilder()
                                                       .setJob(JOB)
                                                       .setGoal(STOP)
                                                       .setState(STOPPED)
                                                       .setContainerId(containerId)
                                                       .setEnv(ENV)
                                                       .build())
    );
    assertEquals(STOPPED, sut.getStatus());
  }

  private String shortJobIdFromContainerName(final String containerName) {
    final int lastUnderscore = containerName.lastIndexOf('_');
    return containerName.substring(0, lastUnderscore).replace('_', ':');
  }

  @Test
  public void verifySupervisorRestartsExitedContainer() throws Exception {
    final String containerId1 = "deadbeef1";
    final String containerId2 = "deadbeef2";

    final ContainerCreation createResponse1 = new ContainerCreation(containerId1);
    final ContainerCreation createResponse2 = new ContainerCreation(containerId2);

    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(createResponse1);

    final ImageInfo imageInfo = new ImageInfo();
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    when(docker.inspectContainer(eq(containerId1))).thenReturn(RUNNING_RESPONSE);

    final SettableFuture<Integer> waitFuture1 = SettableFuture.create();
    final SettableFuture<Integer> waitFuture2 = SettableFuture.create();
    when(docker.waitContainer(containerId1)).thenAnswer(futureAnswer(waitFuture1));
    when(docker.waitContainer(containerId2)).thenAnswer(futureAnswer(waitFuture2));

    // Start the job
    sut.setGoal(START);
    verify(docker, timeout(30000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(30000)).startContainer(eq(containerId1), any(HostConfig.class));
    verify(docker, timeout(30000)).waitContainer(containerId1);

    // Indicate that the container exited
    when(docker.inspectContainer(eq(containerId1))).thenReturn(STOPPED_RESPONSE);
    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(createResponse2);
    when(docker.inspectContainer(eq(containerId2))).thenReturn(RUNNING_RESPONSE);
    waitFuture1.set(1);

    // Verify that the container was restarted
    verify(docker, timeout(30000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(30000)).startContainer(eq(containerId2), any(HostConfig.class));
    verify(docker, timeout(30000)).waitContainer(containerId2);
  }

  public void verifyExceptionSetsTaskStatusToFailed(final Exception exception) throws Exception {
    when(docker.inspectImage(IMAGE)).thenThrow(exception);

    when(retryPolicy.getRetryIntervalMillis()).thenReturn(MINUTES.toMillis(1));

    // Start the job
    sut.setGoal(START);

    verify(retryPolicy, timeout(30000)).getRetryIntervalMillis();

    assertEquals(taskStatusCaptor.getValue().getState(), FAILED);
  }

  @Test
  public void verifyDockerExceptionSetsTaskStatusToFailed() throws Exception {
    verifyExceptionSetsTaskStatusToFailed(new DockerException("FAIL"));
  }

  @Test
  public void verifyRuntimeExceptionSetsTaskStatusToFailed() throws Exception {
    verifyExceptionSetsTaskStatusToFailed(new RuntimeException("FAIL"));
  }

  private Answer<?> futureAnswer(final SettableFuture<?> future) {
    return new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        return future.get();
      }
    };
  }

}
