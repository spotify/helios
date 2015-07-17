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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents something that has happened to a deployment group.
 *
 * A typical JSON representation of an event might be:
 * <pre>
 * {
 *   "action" : DEPLOY_NEW_JOB,
 *   "job" : foo:0.1.0,
 *   "target": "myhost"
 *   "rolloutTaskStatus" : OK,
 *   "deploymentGroup" : { #... see definition of DeploymentGroup },
 *   "deploymentGroupState" : ROLLING_OUT,
 *   "timestamp" : 1410308461448
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeploymentGroupEvent {

  private final RolloutTask.Action action;
  private final JobId jobId;
  private final String target;
  private final RolloutTask.Status rolloutTaskStatus;
  private final DeploymentGroup deploymentGroup;
  private final DeploymentGroupStatus.State deploymentGroupState;
  private final long timestamp;

  /**
   * Constructor.
   *
   * @param action {@link RolloutTask.Action}.
   * @param jobId  {@link JobId}.
   * @param target The target of the action.
   * @param rolloutTaskStatus  The status of the task at the point of the event.
   *                           See {@link RolloutTask.Status}
   * @param deploymentGroup {@link DeploymentGroup}
   * @param deploymentGroupState {@link DeploymentGroupStatus.State}
   * @param timestamp The timestamp of the event.
   */
  public DeploymentGroupEvent(
      @JsonProperty("action") final RolloutTask.Action action,
      @JsonProperty("job") final JobId jobId,
      @JsonProperty("target") final String target,
      @JsonProperty("rolloutTaskStatus") final RolloutTask.Status rolloutTaskStatus,
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup,
      @JsonProperty("deploymentGroupState") final DeploymentGroupStatus.State deploymentGroupState,
      @JsonProperty("timestamp") final long timestamp) {
    this.action = action;
    this.jobId = jobId;
    this.target = target;
    this.rolloutTaskStatus = rolloutTaskStatus;
    this.deploymentGroup = deploymentGroup;
    this.deploymentGroupState = deploymentGroupState;
    this.timestamp = timestamp;
  }

  private DeploymentGroupEvent(final Builder builder) {
    this.deploymentGroup = checkNotNull(builder.deploymentGroup);
    this.deploymentGroupState = checkNotNull(builder.deploymentGroupState);
    this.timestamp = System.currentTimeMillis();

    // Optional
    this.action = builder.action;
    this.jobId = builder.jobId;
    this.target = builder.target;
    this.rolloutTaskStatus = builder.rolloutTaskStatus;
  }

  public RolloutTask.Action getAction() {
    return action;
  }

  public JobId getJobId() {
    return jobId;
  }

  public String getTarget() {
    return target;
  }

  public RolloutTask.Status getRolloutTaskStatus() {
    return rolloutTaskStatus;
  }

  public DeploymentGroup getDeploymentGroup() {
    return deploymentGroup;
  }

  public DeploymentGroupStatus.State getDeploymentGroupState() {
    return deploymentGroupState;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private RolloutTask.Action action;
    private JobId jobId;
    private String target;
    private RolloutTask.Status rolloutTaskStatus;
    private DeploymentGroup deploymentGroup;
    private DeploymentGroupStatus.State deploymentGroupState;

    public Builder setAction(final RolloutTask.Action action) {
      this.action = action;
      return this;
    }

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setTarget(final String target) {
      this.target = target;
      return this;
    }

    public Builder setRolloutTaskStatus(final RolloutTask.Status rolloutTaskStatus) {
      this.rolloutTaskStatus = rolloutTaskStatus;
      return this;
    }

    public Builder setDeploymentGroup(final DeploymentGroup deploymentGroup) {
      this.deploymentGroup = deploymentGroup;
      return this;
    }

    public Builder setDeploymentGroupState(final DeploymentGroupStatus.State deploymentGroupState) {
      this.deploymentGroupState = deploymentGroupState;
      return this;
    }

    public DeploymentGroupEvent build() {
      return new DeploymentGroupEvent(this);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(DeploymentGroupEvent.class)
        .add("action", action)
        .add("jobId", jobId)
        .add("target", target)
        .add("rolloutTaskStatus", rolloutTaskStatus)
        .add("deploymentGroup", deploymentGroup)
        .add("deploymentGroupState", deploymentGroupState)
        .add("timestamp", timestamp)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeploymentGroupEvent that = (DeploymentGroupEvent) o;

    if (timestamp != that.timestamp) {
      return false;
    }
    if (action != that.action) {
      return false;
    }
    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (target != null ? !target.equals(that.target) : that.target != null) {
      return false;
    }
    if (rolloutTaskStatus != that.rolloutTaskStatus) {
      return false;
    }
    if (deploymentGroup != null ? !deploymentGroup.equals(that.deploymentGroup)
                                : that.deploymentGroup != null) {
      return false;
    }
    return deploymentGroupState == that.deploymentGroupState;

  }

  @Override
  public int hashCode() {
    int result = action != null ? action.hashCode() : 0;
    result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
    result = 31 * result + (target != null ? target.hashCode() : 0);
    result = 31 * result + (rolloutTaskStatus != null ? rolloutTaskStatus.hashCode() : 0);
    result = 31 * result + (deploymentGroup != null ? deploymentGroup.hashCode() : 0);
    result = 31 * result + (deploymentGroupState != null ? deploymentGroupState.hashCode() : 0);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }
}
