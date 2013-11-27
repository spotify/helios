package com.spotify.helios.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.ThrottleState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlapController {
  private static class Exit {
    private final long timestamp;
    private final ThrottleState throttle;

    public Exit(long timestamp, TaskStatus.ThrottleState throttle) {
      this.timestamp = timestamp;
      this.throttle = throttle;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public ThrottleState getThrottle() {
      return throttle;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FlapController.class);

  /** Number of restarts in the time period to consider the job flapping */
  private static final int DEFAULT_FLAPPING_RESTART_COUNT = 10;
  /** If total runtime of the container over the last n restarts is less than this, we throttle. */
  private static final long DEFAULT_FLAPPING_TIME_RANGE_MILLIS = 60000;

  private final int restartCount;
  private final long timeRangeMillis;
  private final RestartPolicy restartPolicy;
  private final JobId jobId;
  private final Clock clock;

  private volatile boolean isFlapping;
  private volatile ImmutableList<Exit> lastExits = ImmutableList.<Exit>of();

  private FlapController(final JobId jobId, final int flappingRestartCount, final long flappingTimeRangeMillis,
                         final RestartPolicy restartPolicy, final Clock clock) {
    this.restartCount = flappingRestartCount;
    this.timeRangeMillis = flappingTimeRangeMillis;
    this.jobId = jobId;
    this.restartPolicy = restartPolicy;
    this.clock = clock;
  }

  public void jobDied(ThrottleState throttle) {
    // The CAS-loop here might be overkill...
    ImmutableList<Exit> newExits;

    List<Exit> trimmed = Lists.newArrayList(lastExits);

    while (trimmed.size() >= restartCount) {
      trimmed.remove(0);
    }

    newExits = ImmutableList.<Exit>builder()
        .addAll(trimmed)
        .add(new Exit(clock.now().getMillis(), throttle))
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
      deltaT -= restartPolicy.restartThrottle((newExits.get(i).getThrottle()));
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

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private JobId jobId;
    private int restartCount = DEFAULT_FLAPPING_RESTART_COUNT;
    private long timeRangeMillis = DEFAULT_FLAPPING_TIME_RANGE_MILLIS;
    private Clock clock = new SystemClock();
    private RestartPolicy restartPolicy;

    private Builder() { }

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setRestartPolicy(final RestartPolicy restartPolicy) {
      this.restartPolicy = restartPolicy;
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
      return new FlapController(jobId, restartCount, timeRangeMillis, restartPolicy, clock);
    }
  }
}
