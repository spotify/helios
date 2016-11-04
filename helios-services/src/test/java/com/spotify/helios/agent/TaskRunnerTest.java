/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.Version;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.Job;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class TaskRunnerTest {

  private static final String IMAGE = "spotify:17";
  private static final Job JOB = Job.newBuilder()
      .setName("foobar")
      .setCommand(asList("foo", "bar"))
      .setImage(IMAGE)
      .setVersion("4711")
      .build();
  private static final String HOST = "HOST";

  @Mock private DockerClient mockDocker;
  @Mock private StatusUpdater statusUpdater;
  @Mock private Clock clock;
  @Mock private ContainerDecorator containerDecorator;

  @Test
  public void test() throws Throwable {
    final TaskRunner tr = TaskRunner.builder()
        .delayMillis(0)
        .config(TaskConfig.builder()
                    .namespace("test")
                    .host(HOST)
                    .job(JOB)
                    .containerDecorators(ImmutableList.of(containerDecorator))
                    .build())
        .docker(mockDocker)
        .listener(new TaskRunner.NopListener())
        .imagePuller(image -> { })
        .build();

    tr.run();

    try {
      tr.resultFuture().get();
      fail("this should throw");
    } catch (Exception t) {
      assertTrue(t instanceof ExecutionException);
      assertEquals(HeliosRuntimeException.class, t.getCause().getClass());
    }
  }

  @Test
  public void testPullsAreSerializedWithOldDocker() throws Throwable {
    assertFalse("concurrent calls to docker.pull with a version where it causes issues",
                arePullsConcurrent("1.6.2"));
  }

  @Test
  public void testPullsAreConcurrentWithNewerDocker() throws Throwable {
    assertTrue("calls to docker.pull were unnecessarily serialized",
               arePullsConcurrent("1.9.0-rc1"));
  }

  private boolean arePullsConcurrent(final String dockerVersion)
      throws DockerException, InterruptedException, ExecutionException {
    final Version version = mock(Version.class);
    doReturn(dockerVersion).when(version).version();

    doReturn(version).when(mockDocker).version();

    final AtomicInteger pullers = new AtomicInteger();
    final AtomicBoolean concurrentPullsIssued = new AtomicBoolean(false);
    final ImagePuller imagePuller = image -> {
      try {
        if (pullers.incrementAndGet() > 1) {
          concurrentPullsIssued.set(true);
        }

        Thread.sleep(5000);
      } finally {
        pullers.decrementAndGet();
      }
    };

    final TaskRunner tr = TaskRunner.builder()
        .delayMillis(0)
        .config(TaskConfig.builder()
                    .namespace("test")
                    .host(HOST)
                    .job(JOB)
                    .containerDecorators(ImmutableList.of(containerDecorator))
                    .build())
        .docker(mockDocker)
        .listener(new TaskRunner.NopListener())
        .imagePuller(imagePuller)
        .build();

    final TaskRunner tr2 = TaskRunner.builder()
        .delayMillis(0)
        .config(TaskConfig.builder()
                    .namespace("test")
                    .host(HOST)
                    .job(JOB)
                    .containerDecorators(ImmutableList.of(containerDecorator))
                    .build())
        .docker(mockDocker)
        .listener(new TaskRunner.NopListener())
        .imagePuller(imagePuller)
        .build();

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<?> future = executor.submit(tr::run);
    final Future<?> future2 = executor.submit(tr2::run);

    future.get();
    future2.get();

    return concurrentPullsIssued.get();
  }

  // TODO (dxia) Move this test into a test class for DockerClientImagePuller
//  @Test
//  public void testPullTimeoutVariation() throws Throwable {
////    doThrow(new DockerTimeoutException("x", new URI("http://example.com"), null))
////        .when(mockDocker).pull(IMAGE);
////
////    doThrow()
////        .when(mockDocker).inspectImage(IMAGE);
//
//    final TaskRunner tr = TaskRunner.builder()
//        .delayMillis(0)
//        .config(TaskConfig.builder()
//                    .namespace("test")
//                    .host(HOST)
//                    .job(JOB)
//                    .containerDecorators(ImmutableList.of(containerDecorator))
//                    .build())
//        .docker(mockDocker)
//        .listener(new TaskRunner.NopListener())
//        .imagePuller(image -> {
//          throw new ImageNotFoundException("not found");
//        })
//        .build();
//
//    tr.run();
//
//    try {
//      tr.resultFuture().get();
//      fail("this should throw");
//    } catch (Exception t) {
//      assertTrue(t instanceof ExecutionException);
//      assertEquals(ImagePullFailedException.class, t.getCause().getClass());
//    }
//  }

  @Test
  public void testContainerNotRunningVariation() throws Throwable {
    final TaskRunner.NopListener mockListener = mock(TaskRunner.NopListener.class);
    final ImageInfo mockImageInfo = mock(ImageInfo.class);
    final ContainerCreation mockCreation = mock(ContainerCreation.class);
    final HealthChecker mockHealthChecker = mock(HealthChecker.class);

    final ContainerInfo stopped = new ContainerInfo() {
      @Override
      public ContainerState state() {
        final ContainerState state = mock(ContainerState.class);
        when(state.running()).thenReturn(false);
        when(state.error()).thenReturn("container is a potato");
        return state;
      }
    };

    when(mockCreation.id()).thenReturn("potato");
    when(mockDocker.inspectContainer(anyString())).thenReturn(stopped);
    when(mockDocker.inspectImage(IMAGE)).thenReturn(mockImageInfo);
    when(mockDocker.createContainer(any(ContainerConfig.class), anyString()))
            .thenReturn(mockCreation);
    when(mockHealthChecker.check(anyString())).thenReturn(false);

    final TaskRunner tr = TaskRunner.builder()
        .delayMillis(0)
        .config(TaskConfig.builder()
                    .namespace("test")
                    .host(HOST)
                    .job(JOB)
                    .containerDecorators(ImmutableList.of(containerDecorator))
                    .build())
        .docker(mockDocker)
        .listener(mockListener)
        .healthChecker(mockHealthChecker)
        .imagePuller(image -> { })
        .build();

    tr.run();

    try {
      tr.resultFuture().get();
      fail("this should throw");
    } catch (Exception t) {
      assertTrue(t instanceof ExecutionException);
      assertEquals(RuntimeException.class, t.getCause().getClass());
      verify(mockListener).failed(t.getCause(), "container is a potato");
    }
  }
}
