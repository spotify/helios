/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.Image;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SupervisorTest {

  final Executor executor = Executors.newCachedThreadPool();

  static final String REPOSITORY = "testStartStop";
  static final String TAG = "17";
  static final String IMAGE = REPOSITORY + ":" + TAG;
  static final String NAME = "testStartStop";
  static final JobId JOB_ID = new JobId("test", "job", "deadbeef");
  static final List<String> COMMAND = asList("foo", "bar");
  static final String VERSION = "4711";
  static final Job DESCRIPTOR = Job.newBuilder()
      .setName(NAME)
      .setCommand(COMMAND)
      .setImage(IMAGE)
      .setVersion(VERSION)
      .build();
  static final Map<String, String> ENV = ImmutableMap.of("foo", "17",
                                                         "bar", "4711");
  static final Set<String> EXPECTED_CONTAINER_ENV = ImmutableSet.of("foo=17", "bar=4711");

  static final Image DOCKER_IMAGE = new Image() {{
    repository = IMAGE;
    tag = TAG;
    id = "badf00d";
  }};
  static final List<Image> DOCKER_IMAGES = asList(DOCKER_IMAGE);

  @Mock AgentModel model;
  @Mock AsyncDockerClient docker;

  @Captor ArgumentCaptor<ContainerConfig> containerConfigCaptor;
  @Captor ArgumentCaptor<String> containerNameCaptor;

  Supervisor sut;

  @Before
  public void setup() {
    sut = Supervisor.newBuilder()
        .setJobId(JOB_ID)
        .setDescriptor(DESCRIPTOR)
        .setModel(model)
        .setDockerClient(docker)
        .setRestartIntervalMillis(10)
        .setRetryIntervalMillis(10)
        .setEnvVars(ENV)
        .build();
    when(docker.getImages(IMAGE)).thenReturn(immediateFuture(DOCKER_IMAGES));

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
    }).when(model).setTaskStatus(eq(JOB_ID), any(TaskStatus.class));
    when(model.getTaskStatus(eq(JOB_ID))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final JobId jobId = (JobId) invocationOnMock.getArguments()[0];
        return statusMap.get(jobId);
      }
    });
  }

  @Test
  public void verifySupervisorStartsAndStopsDockerContainer() throws InterruptedException {
    final String containerId = "deadbeef";

    final ContainerCreateResponse createResponse = new ContainerCreateResponse() {{
      id = containerId;
    }};

    final ContainerInspectResponse runningResponse = new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = true;
      }};
    }};

    final ContainerInspectResponse stoppedResponse = new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = false;
      }};
    }};

    final SettableFuture<ContainerCreateResponse> createFuture = SettableFuture.create();
    when(docker.createContainer(any(ContainerConfig.class),
                                any(String.class))).thenReturn(createFuture);
    final SettableFuture<Void> startFuture = SettableFuture.create();
    when(docker.startContainer(containerId)).thenReturn(startFuture);
    final SettableFuture<Integer> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenReturn(waitFuture);

    // Start the job
    sut.start();

    // Verify that the container is created
    verify(docker, timeout(1000)).createContainer(containerConfigCaptor.capture(),
                                                  containerNameCaptor.capture());
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(new TaskStatus(DESCRIPTOR, CREATING, null)));
    assertEquals(CREATING, sut.getStatus());
    createFuture.set(createResponse);
    final ContainerConfig containerConfig = containerConfigCaptor.getValue();
    assertEquals(IMAGE, containerConfig.getImage());
    assertEquals(EXPECTED_CONTAINER_ENV, ImmutableSet.copyOf(containerConfig.getEnv()));
    final String containerName = containerNameCaptor.getValue();
    final UUID uuid = uuidFromContainerName(containerName);
    assertEquals(DESCRIPTOR.getId(), jobIdFromContainerName(containerName));

    // Verify that the container is started
    verify(docker, timeout(1000)).startContainer(containerId);
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(new TaskStatus(DESCRIPTOR, STARTING,
                                                                 containerId)));
    assertEquals(STARTING, sut.getStatus());
    startFuture.set(null);

    verify(docker, timeout(1000)).waitContainer(containerId);
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(new TaskStatus(DESCRIPTOR, RUNNING,
                                                                 containerId)));
    assertEquals(RUNNING, sut.getStatus());

    // Stop the job
    when(docker.inspectContainer(eq(containerId))).thenReturn(immediateFuture(runningResponse));
    final SettableFuture<Void> killFuture = SettableFuture.create();
    when(docker.kill(eq(containerId))).thenReturn(killFuture);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        // TODO (dano): Make Supervisor.stop() asynchronous
        sut.stop();
      }
    });
    verify(docker, timeout(100000)).kill(eq(containerId));

    // Change docker container state to stopped when it's killed
    when(docker.inspectContainer(eq(containerId))).thenReturn(immediateFuture(stoppedResponse));
    killFuture.set(null);

    // Verify that the stopped state is signalled
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(new TaskStatus(DESCRIPTOR, STOPPED,
                                                                 containerId)));
    assertEquals(STOPPED, sut.getStatus());
  }

  private UUID uuidFromContainerName(final String containerName) {
    final int lastColon = containerName.lastIndexOf(':');
    final String uuid = containerName.substring(lastColon + 1);
    return UUID.fromString(uuid);
  }

  private JobId jobIdFromContainerName(final String containerName) {
    final int lastColon = containerName.lastIndexOf(':');
    return JobId.fromString(containerName.substring(0, lastColon));
  }

  @Test
  public void verifySupervisorRestartsExitedContainer() throws InterruptedException {
    final String containerId1 = "deadbeef1";
    final String containerId2 = "deadbeef2";
    final ContainerCreateResponse createResponse1 = new ContainerCreateResponse() {{
      id = containerId1;
    }};
    final ContainerCreateResponse createResponse2 = new ContainerCreateResponse() {{
      id = containerId2;
    }};
    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(immediateFuture(createResponse1));
    when(docker.startContainer(containerId1))
        .thenReturn(immediateFuture((Void) null));
    final SettableFuture<Integer> waitFuture1 = SettableFuture.create();
    final SettableFuture<Integer> waitFuture2 = SettableFuture.create();
    when(docker.waitContainer(containerId1)).thenReturn(waitFuture1);
    when(docker.waitContainer(containerId2)).thenReturn(waitFuture2);

    // Start the job
    sut.start();
    verify(docker, timeout(1000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(1000)).startContainer(containerId1);
    verify(docker, timeout(1000)).waitContainer(containerId1);

    // Indicate that the container exited
    final ContainerInspectResponse stoppedResponse = new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = false;
      }};
    }};
    when(docker.inspectContainer(eq(containerId1))).thenReturn(immediateFuture(stoppedResponse));
    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(immediateFuture(createResponse2));
    when(docker.startContainer(containerId2))
        .thenReturn(immediateFuture((Void) null));
    waitFuture1.set(1);

    // Verify that the container was restarted
    verify(docker, timeout(1000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(1000)).startContainer(containerId2);
    verify(docker, timeout(1000)).waitContainer(containerId2);
  }
}
