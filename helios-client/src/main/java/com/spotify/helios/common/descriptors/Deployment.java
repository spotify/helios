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

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

/**
 * Basically, a pair of {@link JobId} and {@link Goal}.  This is different than {@link Task}
 * which has a {@link Job} and not a {@link JobId}.
 *
 * A typical JSON representation might be:
 * <pre>
 * {
 *   "job": "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265",
 *   "goal": "START",
 * }
 * </pre>
 */
public class Deployment extends Descriptor {

  public static final String EMTPY_DEPLOYER_USER = null;
  private final JobId jobId;
  private final Goal goal;
  private final String deployerUser;

  /**
   * Constructor
   *
   * @param jobId The id of the job.
   * @param goal The desired state (i.e. goal) of the task/deployment.
   * @param deployerUser The user doing the deployment.
   */
  public Deployment(@JsonProperty("job") final JobId jobId,
                    @JsonProperty("goal") final Goal goal,
                    @JsonProperty("deployerUser") @Nullable final String deployerUser) {
    this.jobId  = jobId;
    this.goal = goal;
    this.deployerUser = deployerUser;
  }

  public static Deployment of(final JobId jobId, final Goal goal) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .build();
  }

  public static Deployment of(final JobId jobId, final Goal goal, final String deployerUser) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .setDeployerUser(deployerUser)
        .build();
  }
  public JobId getJobId() {
    return jobId;
  }

  public Goal getGoal() {
    return goal;
  }

  public String getDeployerUser() {
    return deployerUser;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("jobId",  jobId)
        .add("goal", goal)
        .add("deployerUser", deployerUser)
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

    final Deployment that = (Deployment) o;

    if (goal != that.goal) {
      return false;
    }
    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (deployerUser != null
        ? !deployerUser.equals(that.deployerUser)
        : that.deployerUser != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = jobId != null ? jobId.hashCode() : 0;
    result = 31 * result + (goal != null ? goal.hashCode() : 0);
    result = 31 * result + (deployerUser != null ? deployerUser.hashCode() : 0);
    return result;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setDeployerUser(deployerUser)
        .setGoal(goal)
        .setJobId(jobId);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private JobId jobId;
    private Goal goal;
    private String deployerUser;

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setGoal(final Goal goal) {
      this.goal = goal;
      return this;
    }

    public Builder setDeployerUser(final String deployerUser) {
      this.deployerUser = deployerUser;
      return this;
    }

    public Deployment build() {
      return new Deployment(jobId, goal, deployerUser);
    }
  }
}
