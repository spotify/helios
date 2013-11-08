/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

@JsonInclude(NON_NULL)
public class AgentJob extends Descriptor {

  private final String job;
  private final JobGoal goal;

  public AgentJob(@JsonProperty("job") final String job,
                  @JsonProperty("goal") final JobGoal goal) {
    this.job = job;
    this.goal = goal;
  }

  public static AgentJob of(final String job, final JobGoal goal) {
    return newBuilder()
        .setJob(job)
        .setGoal(goal)
        .build();
  }

  public String getJob() {
    return job;
  }

  public JobGoal getGoal() {
    return goal;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("goal", goal)
        .toString();
  }

  @SuppressWarnings("RedundantIfStatement")
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AgentJob agentJob = (AgentJob) o;

    if (goal != agentJob.goal) {
      return false;
    }
    if (job != null ? !job.equals(agentJob.job) : agentJob.job != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (goal != null ? goal.hashCode() : 0);
    return result;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String job;
    private JobGoal goal;

    public Builder setJob(final String job) {
      this.job = job;
      return this;
    }

    public Builder setGoal(final JobGoal goal) {
      this.goal = goal;
      return this;
    }

    public AgentJob build() {
      return new AgentJob(job, goal);
    }
  }
}
