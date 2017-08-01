/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.agent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Range.closed;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.TimeUnit;

/**
 * A retry policy that provides exponential backoff with randomization.
 *
 * <p>interval' = interval * (2.0 + rnd(-1.0, 1.0) * randomFactor)
 */
public class BoundedRandomExponentialBackoff implements RetryIntervalPolicy {

  public static final long DEFAULT_MIN_INTERVAL_MILLIS = SECONDS.toMillis(1);
  public static final long DEFAULT_MAX_INTERVAL_MILLIS = SECONDS.toMillis(30);
  public static final float DEFAULT_RANDOMIZATION_FACTOR = 0.5f;

  private final long minIntervalMillis;
  private final long maxIntervalMillis;
  private final float randomizationFactor;

  /**
   * Create a {@link com.spotify.helios.agent.BoundedRandomExponentialBackoff} with custom values.
   *
   * @param minIntervalMillis   The minimum and initial retry interval.
   * @param maxIntervalMillis   The maximum retry interval.
   * @param randomizationFactor The randomization factor.
   */
  public BoundedRandomExponentialBackoff(final long minIntervalMillis, final long maxIntervalMillis,
                                         final float randomizationFactor) {
    checkArgument(closed(0.0f, 1.0f).contains(randomizationFactor),
        "randomization factor must be in in [0.0, 1.0]");
    checkArgument(minIntervalMillis > 0, "min interval must be positive");
    checkArgument(maxIntervalMillis > 0, "max interval must be positive");
    checkArgument(minIntervalMillis <= maxIntervalMillis,
        "min interval must be less or equal to max interval");

    this.minIntervalMillis = minIntervalMillis;
    this.maxIntervalMillis = maxIntervalMillis;
    this.randomizationFactor = randomizationFactor;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public RetryScheduler newScheduler() {
    return new RetryScheduler() {

      private long intervalMillis;

      @Override
      public long currentMillis() {
        if (intervalMillis == 0) {
          return minIntervalMillis;
        } else {
          return intervalMillis;
        }
      }

      @Override
      public long nextMillis() {
        if (intervalMillis == 0) {
          intervalMillis = minIntervalMillis;
        } else {
          final double factor = (2.0 + random(-1.0, 1.0) * randomizationFactor);
          final long newIntervalMillis = (long) (intervalMillis * factor);
          intervalMillis = max(minIntervalMillis, min(maxIntervalMillis, newIntervalMillis));
        }
        return intervalMillis;
      }

      private double random(final double min, final double max) {
        return min + (max - min) * Math.random();
      }
    };
  }

  public static class Builder {

    private Builder() {

    }

    private long minIntervalMillis = DEFAULT_MIN_INTERVAL_MILLIS;
    private long maxIntervalMillis = DEFAULT_MAX_INTERVAL_MILLIS;
    private float randomizationFactor = DEFAULT_RANDOMIZATION_FACTOR;

    public Builder setMinInterval(final long minInterval, final TimeUnit timeUnit) {
      return setMinIntervalMillis(timeUnit.toMillis(minInterval));
    }

    public Builder setMaxInterval(final long maxInterval, final TimeUnit timeUnit) {
      return setMaxIntervalMillis(timeUnit.toMillis(maxInterval));
    }

    public Builder setMinIntervalMillis(final long minIntervalMillis) {
      this.minIntervalMillis = minIntervalMillis;
      return this;
    }

    public Builder setMaxIntervalMillis(final long maxIntervalMillis) {
      this.maxIntervalMillis = maxIntervalMillis;
      return this;
    }

    public Builder setRandomizationFactor(final float randomizationFactor) {
      this.randomizationFactor = randomizationFactor;
      return this;
    }

    public BoundedRandomExponentialBackoff build() {
      return new BoundedRandomExponentialBackoff(minIntervalMillis, maxIntervalMillis,
          randomizationFactor);
    }


  }
}
