/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

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
