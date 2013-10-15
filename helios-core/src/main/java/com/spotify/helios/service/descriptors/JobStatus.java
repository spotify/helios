/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobStatus extends Descriptor {

  public enum State {
    CREATED,
    STARTING,
    RUNNING,
    EXITED,
    DESTROYED
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

  @Override public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("state", state)
        .add("id", id)
        .toString();
  }
}
