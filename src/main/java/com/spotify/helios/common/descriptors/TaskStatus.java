/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskStatus extends Descriptor {

  public enum State {
    CREATING,
    STARTING,
    RUNNING,
    EXITED,
    STOPPED
  }

  private final Job job;
  private final State state;
  private final String containerId;
  private final ThrottleState throttled;

  public TaskStatus(@JsonProperty("job") final Job job,
                    @JsonProperty("state") final State state,
                    @Nullable @JsonProperty("containerId") final String containerId,
                    @JsonProperty("throttled") final ThrottleState throttled) {
    this.job = checkNotNull(job, "job");
    this.state = checkNotNull(state, "state");
    this.containerId = containerId;
    this.throttled = throttled;
  }

  public ThrottleState getThrottled() {
    return throttled;
  }

  @Nullable
  public String getContainerId() {
    return containerId;
  }

  public State getState() {
    return state;
  }

  public Job getJob() {
    return job;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("state", state)
        .add("containerId", containerId)
        .add("throttled", throttled)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TaskStatus status = (TaskStatus) o;

    if (containerId != null ? !containerId.equals(status.containerId)
                            : status.containerId != null) {
      return false;
    }
    if (job != null ? !job.equals(status.job) : status.job != null) {
      return false;
    }
    if (state != status.state) {
      return false;
    }

    if (throttled != status.throttled) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (containerId != null ? containerId.hashCode() : 0);
    result = 31 * result + ((throttled == null) ? 0 : throttled.hashCode());
    return result;
  }
}
