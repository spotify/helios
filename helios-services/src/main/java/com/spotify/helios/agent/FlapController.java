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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlapController {
  private static final Logger log = LoggerFactory.getLogger(FlapController.class);

  /** Number of restarts in the time period to consider the job flapping */
  private static final int DEFAULT_FLAPPING_RESTART_COUNT = 10;
  /** If total runtime of the container over the last n restarts is less than this, we throttle. */
  private static final long DEFAULT_FLAPPING_TIME_RANGE_MILLIS = 60000;

  private final int restartCount;
  private final long timeRangeMillis;
  private final JobId jobId;
  private final Clock clock;
  private final TaskStatusManager stateUpdater;

  private volatile ImmutableList<Long> lastExits = ImmutableList.of();
  private volatile long mostRecentStartTime;
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
    mostRecentStartTime = clock.now().getMillis();
  }

  public void jobDied() throws InterruptedException {
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

  private void setFlapping(boolean isFlapping) throws InterruptedException {
    if (stateUpdater.isFlapping() != isFlapping) {
      log.info("JobId {} flapping status changed from {} to {}", jobId,
        stateUpdater.isFlapping(), isFlapping);
    } else {
      return;
    }
    stateUpdater.updateFlappingState(isFlapping);
  }

  public <T> T waitFuture(ListenableFuture<T> future)
      throws InterruptedException, ExecutionException, HeliosException {
    log.debug("isFlapping is " + isFlapping());
    if (!isFlapping()) {
      return future.get();
    }

    // If we're flapping, see if we this thing lives long enough no not flap anymore
    try {
      log.debug("Time left to unflap is {}", timeLeftToUnflap);
      return future.get(timeLeftToUnflap, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      setFlapping(false);
    }
    // We survived long enough to no longer be considered flapping, now just wait until it dies.
    return future.get();
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
