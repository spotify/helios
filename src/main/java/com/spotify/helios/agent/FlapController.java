package com.spotify.helios.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.JobId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlapController {
  private static class Exit {
    private final boolean prevWasFlapping;
    private final long timestamp;

    public Exit(boolean prevWasFlapping, long timestamp) {
      this.prevWasFlapping = prevWasFlapping;
      this.timestamp = timestamp;
    }

    public boolean isPrevWasFlapping() {
      return prevWasFlapping;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FlapController.class);

  /** Number of restarts in the time period to consider the job flapping */
  private static final int DEFAULT_FLAPPING_RESTART_COUNT = 10;
  /** If total runtime of the container over the last n restarts is less than this, we throttle. */
  private static final long DEFAULT_FLAPPING_TIME_RANGE_MILLIS = 60000;
  /** Restart throttle interval if job is flapping */
  private static final long DEFAULT_FLAPPING_RESTART_THROTTLE_MILLIS = 30000;

  private final int restartCount;
  private final long timeRangeMillis;
  private final long throttleMillis;
  private final JobId jobId;
  private final Clock clock;

  private volatile boolean isFlapping;
  private volatile ImmutableList<Exit> lastExits = ImmutableList.<Exit>of();

  private FlapController(JobId jobId, int flappingRestartCount, long flappingTimeRangeMillis,
                         long throttleMillis, Clock clock) {
    this.restartCount = flappingRestartCount;
    this.timeRangeMillis = flappingTimeRangeMillis;
    this.jobId = jobId;
    this.throttleMillis = throttleMillis;
    this.clock = clock;
  }

  public void jobDied() {
    // The CAS-loop here might be overkill...
    ImmutableList<Exit> newExits;

    List<Exit> trimmed = Lists.newArrayList(lastExits);

    while (trimmed.size() >= restartCount) {
      trimmed.remove(0);
    }

    newExits = ImmutableList.<Exit>builder()
        .addAll(trimmed)
        .add(new Exit(isFlapping, clock.now().getMillis()))
        .build();

    lastExits = newExits;

    // Not restarted enough times to be considered flapping
    if (newExits.size() < restartCount) {
      setFlapping(false);
      return;
    }

    // Compute the amount of time between exits, adjusting for the restart throttle
    long totalTime = 0;
    for (int i = 1 ; i < restartCount; i++) {
      long deltaT = newExits.get(i).getTimestamp() - newExits.get(i-1).getTimestamp();
      if (newExits.get(i-1).isPrevWasFlapping()) {
        deltaT -= throttleMillis;
      }
      totalTime += deltaT;
    }

    // If not running enough, we're flapping
    setFlapping(totalTime < timeRangeMillis);
  }

  private void setFlapping(boolean isFlapping) {
    if (this.isFlapping != isFlapping) {
      log.info("JobId {} flapping status changed from {} to {}", jobId,
          this.isFlapping, isFlapping);
    }
    this.isFlapping = isFlapping;
  }

  public boolean isFlapping() {
    return isFlapping;
  }

  public long getThrottleMillis() {
    return throttleMillis;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private JobId jobId;
    private long throttleMillis = DEFAULT_FLAPPING_RESTART_THROTTLE_MILLIS;
    private int restartCount = DEFAULT_FLAPPING_RESTART_COUNT;
    private long timeRangeMillis = DEFAULT_FLAPPING_TIME_RANGE_MILLIS;
    private Clock clock = new SystemClock();

    private Builder() { }

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setThrottleMillis(long throttleMillis) {
      this.throttleMillis = throttleMillis;
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

    public Builder setClock(final Clock clock) {
      this.clock = clock;
      return this;
    }

    public FlapController build() {
      return new FlapController(jobId, restartCount, timeRangeMillis, throttleMillis, clock);
    }
  }
}
