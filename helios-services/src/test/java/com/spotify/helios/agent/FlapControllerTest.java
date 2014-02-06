package com.spotify.helios.agent;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.helios.TestBase;
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
public class FlapControllerTest extends TestBase {
  private static final int LOCK_WAIT_TIME = 10000;

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
  private JobId JOB_ID = new JobId("NAME", "VERSION", "deadbeef");

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
    final CyclicBarrier barrier = new CyclicBarrier(2);
    ListenableFuture<Integer> waitContainer = executor.submit(new Callable<Integer>() {
      @Override public Integer call() throws Exception {
        // runs until we tell it not to
        barrier.await(10, TimeUnit.SECONDS);
        return 3;
      }
    });

    controller.jobStarted();
    when(clock.now()).thenReturn(new Instant(3));

    new Thread(new Runnable() {
      @Override public void run() {
        try {
          long start;

          // wait for the manager's state to change
          synchronized(manager) {
            start = System.currentTimeMillis();
            manager.wait(LOCK_WAIT_TIME);
            assertTrue("manager wait shouldn't take that long",
                System.currentTimeMillis() - start < LOCK_WAIT_TIME);
          }
          assertFalse(controller.isFlapping());

          // tell waitContainer to finish
          start = System.currentTimeMillis();
          barrier.await(10, TimeUnit.SECONDS);
          assertTrue("barrier wait shouldn't take that long",
              System.currentTimeMillis() - start < LOCK_WAIT_TIME);
        } catch (RuntimeException | InterruptedException | BrokenBarrierException
                 | TimeoutException e) {
          e.printStackTrace();
        }
      }
    }).start();

    assertEquals((Integer)3, controller.waitFuture(waitContainer));

    when(clock.now()).thenReturn(new Instant(23));
    controller.jobDied();
    assertFalse(controller.isFlapping());
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