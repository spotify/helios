/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class Task extends Descriptor {

  private final Job job;
  private final Goal goal;

  public Task(@JsonProperty("job") final Job job,
              @JsonProperty("goal") final Goal goal) {
    this.job = checkNotNull(job, "job");
    this.goal = checkNotNull(goal, "goal");
  }

  public Goal getGoal() {
    return goal;
  }

  public Job getJob() {
    return job;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Task task = (Task) o;

    if (goal != task.goal) {
      return false;
    }
    if (job != null ? !job.equals(task.job) : task.job != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = goal != null ? goal.hashCode() : 0;
    result = 31 * result + (job != null ? job.hashCode() : 0);
    return result;
  }
}
