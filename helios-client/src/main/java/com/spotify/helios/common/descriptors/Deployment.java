/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public class Deployment extends Descriptor {

  private final JobId jobId;
  private final Goal goal;

  public Deployment(@JsonProperty("job") final JobId jobId,
                    @JsonProperty("goal") final Goal goal) {
    this.jobId  = jobId;
    this.goal = goal;
  }

  public static Deployment of(final JobId jobId, final Goal goal) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .build();
  }

  public JobId getJobId() {
    return jobId;
  }

  public Goal getGoal() {
    return goal;
  }

  @Override
  public String toString() {
    return jobId + "|" + goal;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Deployment that = (Deployment) o;

    if (goal != that.goal) {
      return false;
    }
    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = jobId != null ? jobId.hashCode() : 0;
    result = 31 * result + (goal != null ? goal.hashCode() : 0);
    return result;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private JobId jobId;
    private Goal goal;

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setGoal(final Goal goal) {
      this.goal = goal;
      return this;
    }

    public Deployment build() {
      return new Deployment(jobId, goal);
    }
  }
}
