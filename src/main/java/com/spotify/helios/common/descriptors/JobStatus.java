/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobStatus extends Descriptor {

  public enum State {
    CREATED,
    STARTING,
    RUNNING,
    EXITED,
    STOPPED
  }

  private final JobDescriptor job;
  private final State state;
  private final String id;

  public JobStatus(@JsonProperty("job") final JobDescriptor job,
                   @JsonProperty("state") final State state,
                   @JsonProperty("id") final String id) {
    this.id = id;
    this.state = state;
    this.job = job;
  }

  public String getId() {
    return id;
  }

  public State getState() {
    return state;
  }

  public JobDescriptor getJob() {
    return job;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("state", state)
        .add("id", id)
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

    final JobStatus status = (JobStatus) o;

    if (id != null ? !id.equals(status.id) : status.id != null) {
      return false;
    }
    if (job != null ? !job.equals(status.job) : status.job != null) {
      return false;
    }
    if (state != status.state) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (id != null ? id.hashCode() : 0);
    return result;
  }
}
