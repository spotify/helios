/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.Image;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;

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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.JobStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STOPPED;
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
  static final List<String> COMMAND = asList("foo", "bar");
  static final String VERSION = "4711";
  static final JobDescriptor DESCRIPTOR = JobDescriptor.newBuilder()
      .setName(NAME)
      .setCommand(COMMAND)
      .setImage(IMAGE)
      .setVersion(VERSION)
      .build();

  static final Image DOCKER_IMAGE = new Image() {{
    repository = IMAGE;
    tag = TAG;
    id = "badf00d";
  }};
  static final List<Image> DOCKER_IMAGES = asList(DOCKER_IMAGE);

  @Mock State state;
  @Mock AsyncDockerClient docker;

  @Captor ArgumentCaptor<ContainerConfig> containerConfigCaptor;

  Supervisor sut;

  @Before
  public void setup() {
    sut = Supervisor.newBuilder()
        .setName(NAME)
        .setDescriptor(DESCRIPTOR)
        .setState(state)
        .setDockerClient(docker)
        .setRestartIntervalMillis(10)
        .setRetryIntervalMillis(10)
        .build();
    when(docker.getImages(IMAGE)).thenReturn(immediateFuture(DOCKER_IMAGES));

    final ConcurrentMap<String, JobStatus> statusMap = Maps.newConcurrentMap();
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) {
        final Object[] arguments = invocationOnMock.getArguments();
        final String name = (String) arguments[0];
        final JobStatus status = (JobStatus) arguments[1];
        statusMap.put(name, status);
        return null;
      }
    }).when(state).setJobStatus(eq(NAME), any(JobStatus.class));
    when(state.getJobStatus(eq(NAME))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final String name = (String) invocationOnMock.getArguments()[0];
        return statusMap.get(name);
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
    when(docker.createContainer(any(ContainerConfig.class))).thenReturn(createFuture);
    final SettableFuture<Void> startFuture = SettableFuture.create();
    when(docker.startContainer(containerId)).thenReturn(startFuture);
    final SettableFuture<Integer> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenReturn(waitFuture);

    // Start the job
    sut.start();

    // Verify that the container is created
    verify(docker, timeout(1000)).createContainer(containerConfigCaptor.capture());
    verify(state, timeout(1000)).setJobStatus(eq(NAME),
                                              eq(new JobStatus(DESCRIPTOR, CREATING, null)));
    assertEquals(CREATING, sut.getStatus());
    createFuture.set(createResponse);
    final ContainerConfig containerConfig = containerConfigCaptor.getValue();
    assertEquals(IMAGE, containerConfig.getImage());

    // Verify that the container is started
    verify(docker, timeout(1000)).startContainer(containerId);
    verify(state, timeout(1000)).setJobStatus(eq(NAME),
                                              eq(new JobStatus(DESCRIPTOR, STARTING, containerId)));
    assertEquals(STARTING, sut.getStatus());
    startFuture.set(null);

    verify(docker, timeout(1000)).waitContainer(containerId);
    verify(state, timeout(1000)).setJobStatus(eq(NAME),
                                              eq(new JobStatus(DESCRIPTOR, RUNNING, containerId)));
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
    verify(state, timeout(1000)).setJobStatus(eq(NAME),
                                              eq(new JobStatus(DESCRIPTOR, STOPPED, containerId)));
    assertEquals(STOPPED, sut.getStatus());
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
    when(docker.createContainer(any(ContainerConfig.class)))
        .thenReturn(immediateFuture(createResponse1));
    when(docker.startContainer(containerId1))
        .thenReturn(immediateFuture((Void) null));
    final SettableFuture<Integer> waitFuture1 = SettableFuture.create();
    final SettableFuture<Integer> waitFuture2 = SettableFuture.create();
    when(docker.waitContainer(containerId1)).thenReturn(waitFuture1);
    when(docker.waitContainer(containerId2)).thenReturn(waitFuture2);

    // Start the job
    sut.start();
    verify(docker, timeout(1000)).createContainer(containerConfigCaptor.capture());
    verify(docker, timeout(1000)).startContainer(containerId1);
    verify(docker, timeout(1000)).waitContainer(containerId1);

    // Indicate that the container exited
    final ContainerInspectResponse stoppedResponse = new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = false;
      }};
    }};
    when(docker.inspectContainer(eq(containerId1))).thenReturn(immediateFuture(stoppedResponse));
    when(docker.createContainer(any(ContainerConfig.class)))
        .thenReturn(immediateFuture(createResponse2));
    when(docker.startContainer(containerId2))
        .thenReturn(immediateFuture((Void) null));
    waitFuture1.set(1);

    // Verify that the container was restarted
    verify(docker, timeout(1000)).createContainer(containerConfigCaptor.capture());
    verify(docker, timeout(1000)).startContainer(containerId2);
    verify(docker, timeout(1000)).waitContainer(containerId2);
  }
}
