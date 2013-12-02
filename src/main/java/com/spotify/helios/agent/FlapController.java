package com.spotify.helios.agent;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus.State;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class FlapController {
  private static final Logger log = LoggerFactory.getLogger(FlapController.class);

  public static final ThreadFactory RUNNER_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("helios-flap-controller-runner-%d").build();

  /** Number of restarts in the time period to consider the job flapping */
  private static final int DEFAULT_FLAPPING_RESTART_COUNT = 10;
  /** If total runtime of the container over the last n restarts is less than this, we throttle. */
  private static final long DEFAULT_FLAPPING_TIME_RANGE_MILLIS = 60000;

  private static final long WAIT_TIME = 3;

  private final int restartCount;
  private final long timeRangeMillis;
  private final JobId jobId;
  private final Clock clock;
  private final TaskStatusManager stateUpdater;

  private final ExecutorService executor = newSingleThreadExecutor(RUNNER_THREAD_FACTORY);
  private final CyclicBarrier barrier = new CyclicBarrier(2);
  private final Object checkerLock = new Object();

  private volatile ImmutableList<Long> lastExits = ImmutableList.<Long>of();
  private volatile long mostRecentStartTime = 0;
  private volatile long timeLeftToUnflap;


  private FlapController(final JobId jobId, final int flappingRestartCount,
                         final long flappingTimeRangeMillis, final Clock clock,
                         final TaskStatusManager stateUpdater) {
    this.restartCount = flappingRestartCount;
    this.timeRangeMillis = flappingTimeRangeMillis;
    this.jobId = jobId;
    this.clock = clock;
    this.stateUpdater = stateUpdater;
  }

  public void jobStarted() {
    if (stateUpdater.isFlapping()) {
      log.debug("In flapping mode, starting a waiter to see if we unflap");
      makeWaiter();
    }
    mostRecentStartTime = clock.now().getMillis();
  }

  public void jobDied() {
    tellWaiterWeDied();
    List<Long> trimmed = Lists.newArrayList(lastExits);

    while (trimmed.size() >= restartCount) {
      trimmed.remove(0);
    }

    lastExits = ImmutableList.<Long>builder()
        .addAll(trimmed)
        .add(clock.now().getMillis() - mostRecentStartTime)
        .build();

    // Not restarted enough times to be considered flapping
    if (lastExits.size() < restartCount) {
      setFlapping(false);
      return;
    }

    int totalRunningTime = 0;
    for (Long exit : lastExits) {
      totalRunningTime += exit;
    }

    long flapDifference = timeRangeMillis - totalRunningTime;
    // If not running enough, we're flapping
    setFlapping(flapDifference > 0);
    if (flapDifference <= 0) {
      timeLeftToUnflap = 0;
    } else {
      // Increase by the 0th exit in lastExits, because it would be rolled
      // off when the task exits next time.
      timeLeftToUnflap = flapDifference + lastExits.get(0);
    }
  }

  private void tellWaiterWeDied() {
    // This can safely run even if the waiter was never started.
    synchronized(checkerLock) {
      checkerLock.notify();
    }
  }

  private void makeWaiter() {
    executor.submit(new Runnable() {
      @Override public void run() {
        State state;
        synchronized (checkerLock) {
          // This is so we don't run into a case where the jobDied runs before we've acquired
          // the checkerLock.
          try {
            barrier.await(WAIT_TIME, TimeUnit.SECONDS);
          } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            throw Throwables.propagate(e);
          }
          // Since we have the checker lock, jobDied can't run until the wait() below.
          try {
            checkerLock.wait(timeLeftToUnflap);
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
          state = stateUpdater.getStatus();
        }
        // Do this outside the synchronized block so we don't block the restart of the task
        // until the updater finishes.
        log.info("updateFlappingState {} {}", state, state != State.RUNNING);
        stateUpdater.updateFlappingState(state != State.RUNNING);
      }
    });

    // Make sure the waiter has the checker lock before we proceed
    try {
      barrier.await(WAIT_TIME, TimeUnit.SECONDS);
    } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
      throw Throwables.propagate(e);
    }
  }

  private void setFlapping(boolean isFlapping) {
    if (stateUpdater.isFlapping() != isFlapping) {
      log.info("JobId {} flapping status changed from {} to {}", jobId,
        stateUpdater.isFlapping(), isFlapping);
    } else {
      return;
    }
    stateUpdater.updateFlappingState(isFlapping);
  }

  public boolean isFlapping() {
    return stateUpdater.isFlapping();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private JobId jobId;
    private int restartCount = DEFAULT_FLAPPING_RESTART_COUNT;
    private long timeRangeMillis = DEFAULT_FLAPPING_TIME_RANGE_MILLIS;
    private Clock clock = new SystemClock();
    private TaskStatusManager statusManager;

    private Builder() { }

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setRestartCount(int restartCount) {
      this.restartCount = restartCount;
      return this;
    }

    public Builder setTimeRangeMillis(final long timeRangeMillis) {
      this.timeRangeMillis = timeRangeMillis;
      return this;
    }

    public Builder setTaskStatusManager(final TaskStatusManager manager) {
      this.statusManager = manager;
      return this;
    }

    public Builder setClock(final Clock clock) {
      this.clock = clock;
      return this;
    }

    public FlapController build() {
      return new FlapController(jobId, restartCount, timeRangeMillis, clock, statusManager);
    }
  }
}
