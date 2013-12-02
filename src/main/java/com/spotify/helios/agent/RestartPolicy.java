package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.ThrottleState;

public class RestartPolicy {
  private static final long DEFAULT_FLAPPING_RESTART_THROTTLE_MILLIS = 30000;
  private static final long DEFAULT_RESTART_INTERVAL_MILLIS = 100;
  private static final long DEFAULT_RETRY_INTERVAL_MILLIS = 1000;

  private final long restartIntervalMillis;
  private final long flappingThrottleMillis;
  private final long retryIntervalMillis;

  public RestartPolicy(long restartIntervalMillis, long flappingThrottleMillis,
                       long retryIntervalMillis) {
    this.restartIntervalMillis = restartIntervalMillis;
    this.flappingThrottleMillis = flappingThrottleMillis;
    this.retryIntervalMillis = retryIntervalMillis;
  }

  public long getRetryIntervalMillis() {
    return retryIntervalMillis;
  }

  public long restartThrottle(ThrottleState throttle) {
    switch (throttle) {
      case NO:
        return restartIntervalMillis;
      case FLAPPING:
        return flappingThrottleMillis;
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

    public RestartPolicy build() {
      return new RestartPolicy(restartIntervalMillis, flappingThrottleMillis, retryIntervalMillis);
    }
  }
}
