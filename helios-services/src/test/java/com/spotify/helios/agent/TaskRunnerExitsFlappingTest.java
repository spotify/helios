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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.Polling;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.NopServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskRunnerExitsFlappingTest {
  private static final long MIN_UNFLAP_TIME = 10L;
  private static final Integer CONTAINER_EXIT_CODE = 42;
  private static final String IMAGE = "spotify:17";
  private static final Job JOB = Job.newBuilder()
      .setName("foobar")
      .setCommand(asList("foo", "bar"))
      .setImage("spotify:17")
      .setVersion("4711")
      .build();
  private static final String HOST = "HOST";
  private static final String CONTAINER_ID = "CONTAINER_ID";

  @Mock private DockerClient mockDocker;
  @Mock private StatusUpdater statusUpdater;
  @Mock private Clock clock;
  @Mock private ContainerDecorator containerDecorator;

  @Test
  public void test() throws Exception {
    when(clock.now()).thenReturn(new Instant(0L));
    final FakeTaskStatusManager manager = new FakeTaskStatusManager();
    // start off flapping already
    final AtomicReference<ThrottleState> throttle = new AtomicReference<ThrottleState>(FLAPPING);
    manager.updateFlappingState(true);

    final FlapController flapController = FlapController.newBuilder()
        .setJobId(JOB.getId())
        .setRestartCount(1)
        .setTimeRangeMillis(MIN_UNFLAP_TIME)
        .setClock(clock)
        .setTaskStatusManager(manager)
        .build();

    final TaskRunner tr = new TaskRunner(
        0,
        new NopServiceRegistrar(),
        JOB,
        TaskConfig.builder()
            .host(HOST)
            .job(JOB)
            .containerDecorator(containerDecorator)
            .build(),
        new NoopSupervisorMetrics(),
        mockDocker,
        flapController,
        throttle,
        statusUpdater,
        null);

    when(mockDocker.inspectImage(IMAGE)).thenReturn(new ImageInfo());
    when(mockDocker.createContainer(any(ContainerConfig.class),
                                    any(String.class))).thenReturn(new ContainerCreation() {
      @Override
      public String id() {
        return "CONTAINER_ID";
      }
    });
    when(mockDocker.inspectContainer(CONTAINER_ID)).thenReturn(new ContainerInfo() {
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
    });

    final SettableFuture<ContainerExit> containerWaitFuture = SettableFuture.create();
    when(mockDocker.waitContainer(CONTAINER_ID)).thenAnswer(new Answer<ContainerExit>() {
      @Override
      public ContainerExit answer(final InvocationOnMock invocation) throws Throwable {
        return containerWaitFuture.get();
      }
    });

    final CyclicBarrier startBarrier = new CyclicBarrier(2);
    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor());

    // run this in the background
    ListenableFuture<?> trFuture = executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          startBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          fail("barrier failure" + e);
        }
        try {
          tr.run();
        } catch (Throwable e) {
          fail("tr threw exception " + e);
        }
      }
    });

    startBarrier.await();

    // wait for flapping state change
    assertNotNull(Polling.await(30, TimeUnit.SECONDS, new Callable<ThrottleState>() {
      @Override
      public ThrottleState call() throws Exception {
        if (manager.isFlapping()) {
          return null;
        }
        return throttle.get();
      }
    }));

    // make it so container ran for 1s, more than the 10ms req'd
    when(clock.now()).thenReturn(new Instant(MIN_UNFLAP_TIME + 1));
    // tell "container" to die
    containerWaitFuture.set(new ContainerExit(CONTAINER_EXIT_CODE));
    // wait for task runner to finish
    trFuture.get();

    assertEquals(CONTAINER_EXIT_CODE, tr.result().get());
    assertEquals(ThrottleState.NO, throttle.get());
  }
}
