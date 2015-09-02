/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class DeploymentGroupStatusResponse {

  public enum Status {
    ROLLING_OUT,
    ACTIVE,
    FAILED,
    IDLE
  }

  public enum RolloutState {
    PENDING,
    DONE,
    FAILED
  }

  public static class HostStatus {

    private final String host;
    private final JobId jobId;
    private final TaskStatus.State taskState;
    private final RolloutState rolloutState;
    private final String errMsg;

    public HostStatus(@JsonProperty("host") final String host,
                      @JsonProperty("jobId") @Nullable final JobId jobId,
                      @JsonProperty("taskState") @Nullable final TaskStatus.State taskState,
                      @JsonProperty("rolloutState") @Nullable final RolloutState rolloutState,
                      @JsonProperty("errMsg") @Nullable final String errMsg) {
      this.host = host;
      this.jobId = jobId;
      this.taskState = taskState;
      this.rolloutState = rolloutState;
      this.errMsg = errMsg;
    }

    public String getHost() {
      return host;
    }

    public JobId getJobId() {
      return jobId;
    }

    public TaskStatus.State getTaskState() {
      return taskState;
    }

    public RolloutState getRolloutState() {
      return rolloutState;
    }

    public String getErrMsg() {
      return errMsg;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final HostStatus that = (HostStatus) o;

      if (host != null ? !host.equals(that.host) : that.host != null) {
        return false;
      }
      if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
        return false;
      }
      if (taskState != that.taskState) {
        return false;
      }
      if (rolloutState != that.rolloutState) {
        return false;
      }
      if (errMsg != null ? !errMsg.equals(that.errMsg) : that.errMsg != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = host != null ? host.hashCode() : 0;
      result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
      result = 31 * result + (taskState != null ? taskState.hashCode() : 0);
      result = 31 * result + (rolloutState != null ? rolloutState.hashCode() : 0);
      result = 31 * result + (errMsg != null ? errMsg.hashCode() : 0);
      return result;
    }
  }

  private final DeploymentGroup deploymentGroup;
  private final Status status;
  private final String error;
  private final List<HostStatus> hostStatuses;
  private final DeploymentGroupStatus deploymentGroupStatus;

  public DeploymentGroupStatusResponse(
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup,
      @JsonProperty("status") final Status status,
      @JsonProperty("error") final String error,
      @JsonProperty("hostStatuses") final List<HostStatus> hostStatuses,
      @JsonProperty("deploymentGroupStatus") @Nullable final DeploymentGroupStatus dgs) {
    this.deploymentGroup = deploymentGroup;
    this.status = status;
    this.error = error;
    this.hostStatuses = hostStatuses;
    this.deploymentGroupStatus = dgs;
  }

  public DeploymentGroup getDeploymentGroup() {
    return deploymentGroup;
  }

  public Status getStatus() {
    return status;
  }

  public List<HostStatus> getHostStatuses() {
    return hostStatuses;
  }

  public String getError() {
    return error;
  }

  public DeploymentGroupStatus getDeploymentGroupStatus() {
    return deploymentGroupStatus;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("deploymentGroup", deploymentGroup)
        .add("status", status)
        .add("error", error)
        .add("hostStatuses", hostStatuses)
        .add("deploymentGroupStatus", deploymentGroupStatus)
        .toString();
  }

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DeploymentGroupStatusResponse response = (DeploymentGroupStatusResponse) o;

    if (deploymentGroup != null ? !deploymentGroup.equals(response.deploymentGroup)
                                : response.deploymentGroup != null) {
      return false;
    }
    if (deploymentGroupStatus != null ? !deploymentGroupStatus
        .equals(response.deploymentGroupStatus)
                                      : response.deploymentGroupStatus != null) {
      return false;
    }
    if (error != null ? !error.equals(response.error) : response.error != null) {
      return false;
    }
    if (hostStatuses != null ? !hostStatuses.equals(response.hostStatuses)
                             : response.hostStatuses != null) {
      return false;
    }
    if (status != response.status) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = deploymentGroup != null ? deploymentGroup.hashCode() : 0;
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    result = 31 * result + (hostStatuses != null ? hostStatuses.hashCode() : 0);
    result = 31 * result + (deploymentGroupStatus != null ? deploymentGroupStatus.hashCode() : 0);
    return result;
  }
}
