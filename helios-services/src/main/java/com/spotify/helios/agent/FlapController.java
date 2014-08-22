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

import com.google.common.collect.Queues;

import java.util.Deque;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Tracks container starts and exits and determines if the task is "flapping", which is to say:
 * it's exiting rapidly and repeatedly.
 */
public class FlapController {

  /**
   * Number of restarts in the time period to consider the job flapping
   */
  private static final int DEFAULT_FLAPPING_RESTART_COUNT = 10;
  /**
   * If total runtime of the container over the last n restarts is less than this, we throttle.
   */
  private static final long DEFAULT_FLAPPING_TIME_RANGE_MILLIS = 60000;

  private final int restartCount;
  private final long timeRangeMillis;
  private final Clock clock;
  private final Deque<Long> exits = Queues.newArrayDeque();

  private volatile long startMillis;
  private volatile boolean running;

  private FlapController(final Builder builder) {
    this.restartCount = builder.restartCount;
    this.timeRangeMillis = builder.timeRangeMillis;
    this.clock = checkNotNull(builder.clock, "clock");
  }

  public synchronized void started() {
    running = true;
    startMillis = clock.now().getMillis();
  }

  public synchronized void exited() {
    running = false;
    exits.add(clock.now().getMillis() - startMillis);
    while (exits.size() > restartCount) {
      exits.pop();
    }
  }

  public boolean isFlapping() {
    return millisLeftToUnflap() > 0;
  }

  public long millisLeftToUnflap() {
    if (exits.size() < restartCount) {
      return 0;
    }
    return timeRangeMillis - totalRunningTimeMillis();
  }

  private long totalRunningTimeMillis() {
    int duration = 0;
    for (Long exit : exits) {
      duration += exit;
    }
    if (running) {
      duration += clock.now().getMillis() - startMillis;
    }
    return duration;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static FlapController create() {
    return newBuilder().build();
  }

  public static class Builder {

    private int restartCount = DEFAULT_FLAPPING_RESTART_COUNT;
    private long timeRangeMillis = DEFAULT_FLAPPING_TIME_RANGE_MILLIS;
    private Clock clock = new SystemClock();

    private Builder() { }

    public Builder setRestartCount(final int restartCount) {
      this.restartCount = restartCount;
      return this;
    }

    public Builder setTimeRangeMillis(final long timeRangeMillis) {
      this.timeRangeMillis = timeRangeMillis;
      return this;
    }

    public Builder setClock(final Clock clock) {
      this.clock = clock;
      return this;
    }

    public FlapController build() {
      return new FlapController(this);
    }
  }
}
