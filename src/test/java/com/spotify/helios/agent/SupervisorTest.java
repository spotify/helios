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
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.kpelykh.docker.client.model.ListImage;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.statistics.NoopSupervisorMetrics;

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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
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

  static final String REPOSITORY = "spotify";
  static final String TAG = "17";
  static final String IMAGE = REPOSITORY + ":" + TAG;
  static final String NAME = "foobar";
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

  static final ListImage DOCKER_IMAGE = new ListImage() {{ }};
  static final List<ListImage> DOCKER_IMAGES = asList(DOCKER_IMAGE);

  @Mock AgentModel model;
  @Mock AsyncDockerClient docker;

  @Captor ArgumentCaptor<ContainerConfig> containerConfigCaptor;
  @Captor ArgumentCaptor<String> containerNameCaptor;

  Supervisor sut;

  @Before
  public void setup() {
    RestartPolicy policy = RestartPolicy.newBuilder()
        .setNormalRestartIntervalMillis(10)
        .setRetryIntervalMillis(10)
        .build();
    TaskStatusManagerImpl manager = TaskStatusManagerImpl.newBuilder()
        .setJob(DESCRIPTOR)
        .setJobId(JOB_ID)
        .setModel(model)
        .build();
    sut = Supervisor.newBuilder()
        .setAgentName("AGENT_NAME")
        .setJobId(JOB_ID)
        .setDescriptor(DESCRIPTOR)
        .setModel(model)
        .setDockerClient(docker)
        .setRestartPolicy(policy)
        .setEnvVars(ENV)
        .setFlapController(FlapController.newBuilder()
                               .setJobId(JOB_ID)
                               .setTaskStatusManager(manager)
                               .build())
        .setTaskStatusManager(manager)
        .setCommandWrapper(new NoOpCommandWrapper())
        .setMetrics(new NoopSupervisorMetrics())
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
      networkSettings = new NetworkSettings() {{
        ports = emptyMap();
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
    when(docker.startContainer(eq(containerId), any(HostConfig.class))).thenReturn(startFuture);

    final ImageInspectResponse imageInfo = new ImageInspectResponse();
    when(docker.inspectImage(IMAGE)).thenReturn(immediateFuture(imageInfo));

    final SettableFuture<Integer> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenReturn(waitFuture);

    // Start the job
    sut.start();

    // Verify that the container is created
    verify(docker, timeout(1000)).createContainer(containerConfigCaptor.capture(),
                                                  containerNameCaptor.capture());
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(TaskStatus.newBuilder()
                                                      .setJob(DESCRIPTOR)
                                                      .setState(CREATING)
                                                      .setContainerId(null)
                                                      .setEnv(ENV)
                                                      .build()));
    assertEquals(CREATING, sut.getStatus());
    createFuture.set(createResponse);
    final ContainerConfig containerConfig = containerConfigCaptor.getValue();
    assertEquals(IMAGE, containerConfig.getImage());
    assertEquals(EXPECTED_CONTAINER_ENV, ImmutableSet.copyOf(containerConfig.getEnv()));
    final String containerName = containerNameCaptor.getValue();

    assertEquals(DESCRIPTOR.getId().toShortString(), shortJobIdFromContainerName(containerName));

    // Verify that the container is started
    verify(docker, timeout(1000)).startContainer(eq(containerId), any(HostConfig.class));
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(TaskStatus.newBuilder()
                                                      .setJob(DESCRIPTOR)
                                                      .setState(STARTING)
                                                      .setContainerId(containerId)
                                                      .setEnv(ENV)
                                                      .build()));
    assertEquals(STARTING, sut.getStatus());
    when(docker.inspectContainer(eq(containerId))).thenReturn(immediateFuture(runningResponse));
    startFuture.set(null);

    verify(docker, timeout(1000)).waitContainer(containerId);
    verify(model, timeout(1000)).setTaskStatus(eq(JOB_ID),
                                               eq(TaskStatus.newBuilder()
                                                      .setJob(DESCRIPTOR)
                                                      .setState(RUNNING)
                                                      .setContainerId(containerId)
                                                      .setEnv(ENV)
                                                      .build()));
    assertEquals(RUNNING, sut.getStatus());

    // Stop the job
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
                                               eq(TaskStatus.newBuilder()
                                                      .setJob(DESCRIPTOR)
                                                      .setState(STOPPED)
                                                      .setContainerId(containerId)
                                                      .setEnv(ENV)
                                                      .build()));
    assertEquals(STOPPED, sut.getStatus());
  }

  private String shortJobIdFromContainerName(final String containerName) {
    final int lastUnderscore = containerName.lastIndexOf('_');
    return containerName.substring(0, lastUnderscore).replace('_', ':');
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

    final ContainerInspectResponse runningResponse = new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = true;
      }};
      networkSettings = new NetworkSettings() {{
        ports = emptyMap();
      }};
    }};

    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(immediateFuture(createResponse1));

    when(docker.startContainer(eq(containerId1), any(HostConfig.class)))
        .thenReturn(immediateFuture((Void) null));

    final ImageInspectResponse imageInfo = new ImageInspectResponse();
    when(docker.inspectImage(IMAGE)).thenReturn(immediateFuture(imageInfo));

    when(docker.inspectContainer(eq(containerId1))).thenReturn(immediateFuture(runningResponse));

    final SettableFuture<Integer> waitFuture1 = SettableFuture.create();
    final SettableFuture<Integer> waitFuture2 = SettableFuture.create();
    when(docker.waitContainer(containerId1)).thenReturn(waitFuture1);
    when(docker.waitContainer(containerId2)).thenReturn(waitFuture2);

    // Start the job
    sut.start();
    verify(docker, timeout(1000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(1000)).startContainer(eq(containerId1), any(HostConfig.class));
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
    when(docker.startContainer(eq(containerId2), any(HostConfig.class)))
        .thenReturn(immediateFuture((Void) null));
    when(docker.inspectContainer(eq(containerId2))).thenReturn(immediateFuture(runningResponse));
    waitFuture1.set(1);

    // Verify that the container was restarted
    verify(docker, timeout(1000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(1000)).startContainer(eq(containerId2), any(HostConfig.class));
    verify(docker, timeout(1000)).waitContainer(containerId2);
  }
}
