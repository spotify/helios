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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus.State;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlapControllerTest {
  private static final long LOCK_WAIT_TIME_MILLIS = TimeUnit.MINUTES.toMillis(1);

  // Have to get all fancy with this, so we don't have arbitrary Thread.sleep()s
  // in the test code, and so they can run as fast as possible. This way, we also
  // avoid races nearly altogether, and don't indefinitely stall the test if we did it wrong. If the
  // machine is totally bogged and can't run about 12 instructions in 10 seconds, the test will
  // fail, but at that point we've got other problems.  And yes, it was totally worth it as the
  // test that uses this caught a few bugs that would have been exteremely difficult to find
  // otherwise.
  private static final class TestTaskStatusManager extends FakeTaskStatusManager {
    private volatile boolean updatedIsFlapping;

    @Override
    public void updateFlappingState(boolean isFlapping) {
      // Checks for unexpected spurious calls to updateFlappingState
      if (updatedIsFlapping) {
        throw new RuntimeException("Shouldn't have called this again before clearing");
      }
      synchronized (this) {
        super.updateFlappingState(isFlapping);
        updatedIsFlapping = true;
        this.notify();
      }
    }

    public boolean isUpdatedIsFlapping() {
      return updatedIsFlapping;
    }

    public void clearIsUpdatedIsFlapping() {
      this.updatedIsFlapping = false;
    }
  }

  @Mock private Clock clock;

  private static final JobId JOB_ID = new JobId("NAME", "VERSION", "deadbeef");

  @Test
  public void testRecoveryFromFlappingWhileRunning() throws Exception {
    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor());
    final TestTaskStatusManager manager = new TestTaskStatusManager();
    manager.clearIsUpdatedIsFlapping();

    final FlapController controller = FlapController.newBuilder()
        .setJobId(JOB_ID)
        .setClock(clock)
        .setRestartCount(2)
        .setTimeRangeMillis(20)
        .setTaskStatusManager(manager)
        .build();

    assertFalse(controller.isFlapping());
    when(clock.now()).thenReturn(new Instant(0));

    // get controller into flapping state
    manager.setState(State.EXITED);
    controller.jobStarted();
    controller.jobDied();
    assertFalse(controller.isFlapping());      // not failed enough *yet*
    assertFalse(manager.isUpdatedIsFlapping());

    controller.jobStarted();
    controller.jobDied();
    assertTrue(controller.isFlapping());       // now we failed enough.
    assertTrue(manager.isUpdatedIsFlapping());

    //// See that the state maintains the flapping state.

    // reset the fact that the isFlapping state was updated
    manager.clearIsUpdatedIsFlapping();
    assertEquals((Integer)5, controller.waitFuture(Futures.immediateFuture(5)));
    assertTrue(controller.isFlapping());
    controller.jobDied();
    assertTrue(controller.isFlapping());

    //// Now test that the state will update while the future is running

    // Made all thready because API requires it but done in such a way so that it normally will
    // run really fast.
    manager.clearIsUpdatedIsFlapping();
    final CyclicBarrier entryBarrier = new CyclicBarrier(2);
    final CyclicBarrier exitBarrier = new CyclicBarrier(2);

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(3));

    new Thread(new Runnable() {
      @Override public void run() {
        try {

          // wait for the manager's state to change
          synchronized(manager) {
            waitBarrier(entryBarrier, "manager locked and loaded");
            final long start = System.currentTimeMillis();
            manager.wait(LOCK_WAIT_TIME_MILLIS);
            assertTrue("manager wait shouldn't take that long",
                System.currentTimeMillis() - start < LOCK_WAIT_TIME_MILLIS);
          }
          assertFalse(controller.isFlapping());

          // tell waitContainer to finish
          waitBarrier(exitBarrier, "manager telling `container' to exit");
        } catch (RuntimeException | InterruptedException | BrokenBarrierException
                 | TimeoutException e) {
          e.printStackTrace();
        }
      }
    }).start();

    waitBarrier(entryBarrier, "main entry");

    final Integer sentinel = 8675309;
    assertEquals(sentinel, controller.waitFuture(executor.submit(new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        waitBarrier(exitBarrier, "container exit");
        return sentinel;
      }
    })));

    when(clock.now()).thenReturn(new Instant(23));
    controller.jobDied();
    assertFalse(controller.isFlapping());
  }

  private void waitBarrier(final CyclicBarrier barrier, final String msg)
      throws InterruptedException, BrokenBarrierException, TimeoutException {
    final long start = System.currentTimeMillis();
    barrier.await(LOCK_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
    assertTrue(msg + "barrier wait shouldn't take that long",
        System.currentTimeMillis() - start < LOCK_WAIT_TIME_MILLIS);
  }

  @Test
  public void testEnterAndExitFlapping() throws Exception {
    FakeTaskStatusManager manager = new FakeTaskStatusManager();
    FlapController controller = FlapController.newBuilder()
        .setJobId(JOB_ID)
        .setClock(clock)
        .setRestartCount(2)
        .setTimeRangeMillis(20)
        .setTaskStatusManager(manager)
        .build();

    assertFalse(controller.isFlapping());
    when(clock.now()).thenReturn(new Instant(0));

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(1));
    controller.jobDied(); // 1 second of runtime T=1
    assertFalse(controller.isFlapping());

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(2));
    controller.jobDied(); // total of 2ms of runtime T=2
    assertTrue(controller.isFlapping()); // next time job would start would be at t=7 seconds

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(8));
    controller.jobDied(); // total of 3ms of runtime T=8 (5 of that is throttle)
    assertTrue(controller.isFlapping()); // next time job would start would be at t=13

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(34));
    controller.jobDied(); // ran 21ms additionally here, so should disengage flapping T=34
    assertFalse(controller.isFlapping());
  }
}