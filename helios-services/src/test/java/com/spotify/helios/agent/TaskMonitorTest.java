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

import com.google.common.util.concurrent.SettableFuture;

import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.exceptions.ImagePullFailedException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.ThrottleState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLED_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_MISSING;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_PULL_FAILED;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskMonitorTest {

  @Mock FlapController flapController;
  @Mock StatusUpdater statusUpdater;

  static final JobId JOB_ID = JobId.fromString("foo:bar:deadbeef");

  TaskMonitor sut;

  @Before
  public void setup() {
    sut = new TaskMonitor(JOB_ID, flapController, statusUpdater);
  }

  @After
  public void teardown() {
    sut.close();
  }

  @Test
  public void verifyMonitorPropagatesState() throws Exception {
    sut.pulling();
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).setState(PULLING_IMAGE);
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.pulled();
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).setState(PULLED_IMAGE);
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.creating();
    verify(statusUpdater).setState(CREATING);
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.starting();
    verify(statusUpdater).setState(STARTING);
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.running();
    verify(statusUpdater).setState(RUNNING);
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.exited(4711);
    verify(statusUpdater).setState(EXITED);
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    verify(statusUpdater).update();
    reset(statusUpdater);

    sut.failed(new Exception(), "Error herping derps.");
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).setContainerError("Error herping derps.");
    verify(statusUpdater).update();
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    reset(statusUpdater);

    sut.pullFailed();
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).update();
    verify(statusUpdater, never()).setThrottleState(any(ThrottleState.class));
    reset(statusUpdater);
  }

  @Test
  public void verifyMonitorPropagatesImagePullFailed() throws Exception {
    sut.failed(new ImagePullFailedException("foobar", "failure"), "container error");
    verify(statusUpdater).setThrottleState(IMAGE_PULL_FAILED);
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).setContainerError("container error");
    verify(statusUpdater).update();
  }

  @Test
  public void verifyMonitorPropagatesImageMissing() throws Exception {
    sut.failed(new ImageNotFoundException("foobar", "not found"), "container error");
    verify(statusUpdater).setThrottleState(IMAGE_MISSING);
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).setContainerError("container error");
    verify(statusUpdater).update();
  }

  @Test
  public void verifyMonitorUsesFlapController() {
    sut.running();
    verify(statusUpdater, never()).setThrottleState(FLAPPING);
    verify(flapController).started();

    sut.exited(17);
    verify(flapController).exited();
  }

  @Test
  public void verifyImagePullFailureTrumpsFlappingState() throws Exception {
    when(flapController.isFlapping()).thenReturn(true);
    sut.failed(new ImagePullFailedException("foobar", "failure"), "container error");
    verify(statusUpdater).setThrottleState(IMAGE_PULL_FAILED);
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).setContainerError("container error");
    verify(statusUpdater).update();
  }

  @Test
  public void verifyImageMissingTrumpsFlappingState() throws Exception {
    when(flapController.isFlapping()).thenReturn(true);
    sut.failed(new ImageNotFoundException("foobar", "not found"), "container error");
    verify(statusUpdater).setThrottleState(IMAGE_MISSING);
    verify(statusUpdater).setState(FAILED);
    verify(statusUpdater).setContainerError("container error");
    verify(statusUpdater).update();
  }

  @Test
  public void verifyMonitorRecoversFromFlappingState() {
    final SettableFuture<Boolean> flappingRecoveryFuture = SettableFuture.create();

    when(flapController.isFlapping())
        .thenReturn(true)
        .thenAnswer(futureAnswer(flappingRecoveryFuture));
    when(flapController.millisLeftToUnflap()).thenReturn(10L);
    sut.exited(17);
    verify(flapController).exited();
    verify(statusUpdater).setThrottleState(FLAPPING);

    verify(flapController, timeout(30000).times(2)).isFlapping();
    flappingRecoveryFuture.set(false);
    verify(statusUpdater, timeout(30000)).setThrottleState(NO);
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
