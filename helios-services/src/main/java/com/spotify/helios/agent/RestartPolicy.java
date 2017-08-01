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

import static java.lang.Math.max;

import com.spotify.helios.common.descriptors.ThrottleState;

/**
 * Return the proper docker container restart delay based upon the throttle state of the task.
 */
public class RestartPolicy {
  private static final long DEFAULT_IMAGE_MISSING_THROTTLE_MILLIS = 2 * 60 * 1000; // 2 minutes
  private static final long DEFAULT_FLAPPING_RESTART_THROTTLE_MILLIS = 30 * 1000;  // 30 seconds
  private static final long DEFAULT_RESTART_INTERVAL_MILLIS = 100;
  private static final long DEFAULT_RETRY_INTERVAL_MILLIS = 1000;
  private static final long IMAGE_PULL_FAILED_THROTTLE_MILLIS = 30 * 1000; // 30 seconds

  private final long restartIntervalMillis;
  private final long flappingThrottleMillis;
  private final long retryIntervalMillis;
  private final long imageMissingThrottleMillis;

  public RestartPolicy(long restartIntervalMillis, long flappingThrottleMillis,
                       long retryIntervalMillis, long imageMissingThrottleMillis) {
    this.restartIntervalMillis = restartIntervalMillis;
    this.flappingThrottleMillis = flappingThrottleMillis;
    this.retryIntervalMillis = retryIntervalMillis;
    this.imageMissingThrottleMillis = imageMissingThrottleMillis;
  }

  public long delay(final ThrottleState throttle) {
    return max(retryIntervalMillis, delay0(throttle));
  }

  private long delay0(final ThrottleState throttle) {
    switch (throttle) {
      case NO:
        return restartIntervalMillis;
      case IMAGE_MISSING:
        return imageMissingThrottleMillis;
      case FLAPPING:
        return flappingThrottleMillis;
      case IMAGE_PULL_FAILED:
        return IMAGE_PULL_FAILED_THROTTLE_MILLIS;
      default:
        break;
    }
    throw new IllegalStateException("Should never get here, covered both cases");
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private long restartIntervalMillis = DEFAULT_RESTART_INTERVAL_MILLIS;
    private long flappingThrottleMillis = DEFAULT_FLAPPING_RESTART_THROTTLE_MILLIS;
    private long retryIntervalMillis = DEFAULT_RETRY_INTERVAL_MILLIS;
    private long imageMissingThrottleMillis = DEFAULT_IMAGE_MISSING_THROTTLE_MILLIS;

    private Builder() {}

    public Builder setNormalRestartIntervalMillis(final long interval) {
      this.restartIntervalMillis = interval;
      return this;
    }

    public Builder setFlappingThrottleMills(final long interval) {
      this.flappingThrottleMillis = interval;
      return this;
    }

    public Builder setRetryIntervalMillis(long retryIntervalMillis) {
      this.retryIntervalMillis = retryIntervalMillis;
      return this;
    }

    public Builder setImageMissingThrottleMillis(long imageMissingThrottleMillis) {
      this.imageMissingThrottleMillis = imageMissingThrottleMillis;
      return this;
    }

    public RestartPolicy build() {
      return new RestartPolicy(restartIntervalMillis, flappingThrottleMillis, retryIntervalMillis,
          imageMissingThrottleMillis);
    }
  }
}
