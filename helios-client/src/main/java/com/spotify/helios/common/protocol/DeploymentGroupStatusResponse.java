/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
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

  public static class HostStatus {

    private final String host;
    private final JobId jobId;
    private final TaskStatus.State state;

    public HostStatus(@JsonProperty("host") final String host,
                      @JsonProperty("jobId") @Nullable final JobId jobId,
                      @JsonProperty("state") @Nullable final TaskStatus.State state) {
      this.host = host;
      this.jobId = jobId;
      this.state = state;
    }

    public String getHost() {
      return host;
    }

    public JobId getJobId() {
      return jobId;
    }

    public TaskStatus.State getState() {
      return state;
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
      if (state != that.state) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = host != null ? host.hashCode() : 0;
      result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
      result = 31 * result + (state != null ? state.hashCode() : 0);
      return result;
    }
  }

  private final DeploymentGroupResponse deploymentGroupResponse;
  private final Status status;
  private final String error;
  private final List<HostStatus> hostStatuses;
  private final DeploymentGroupStatus deploymentGroupStatus;
  private final RollingOperationStatus lastRollingOpStatus;

  public DeploymentGroupStatusResponse(
      // Named deploymentGroup rather than deploymentGroupResponse to avoid breaking Helios API.
      @JsonProperty("deploymentGroup") final DeploymentGroupResponse deploymentGroupResponse,
      @JsonProperty("status") final Status status,
      @JsonProperty("error") final String error,
      @JsonProperty("hostStatuses") final List<HostStatus> hostStatuses,
      @JsonProperty("deploymentGroupStatus") @Nullable final DeploymentGroupStatus dgs,
      @JsonProperty("lastRollingOpStatus") @Nullable final RollingOperationStatus lros) {
    this.deploymentGroupResponse = deploymentGroupResponse;
    this.status = status;
    this.error = error;
    this.hostStatuses = hostStatuses;
    this.deploymentGroupStatus = dgs;
    this.lastRollingOpStatus = lros;
  }

  public DeploymentGroupResponse getDeploymentGroupResponse() {
    return deploymentGroupResponse;
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

  public RollingOperationStatus getLastRollingOpStatus() {
    return lastRollingOpStatus;
  }

  @Override
  public String toString() {
    return "DeploymentGroupStatusResponse{" +
           "deploymentGroupResponse=" + deploymentGroupResponse +
           ", status=" + status +
           ", error='" + error + '\'' +
           ", hostStatuses=" + hostStatuses +
           ", deploymentGroupStatus=" + deploymentGroupStatus +
           ", lastRollingOpStatus=" + lastRollingOpStatus +
           '}';
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

    if (deploymentGroupResponse != null ? !deploymentGroupResponse
        .equals(response.deploymentGroupResponse)
                                        : response.deploymentGroupResponse != null) {
      return false;
    }
    if (deploymentGroupStatus != null ? !deploymentGroupStatus
        .equals(response.deploymentGroupStatus)
                                      : response.deploymentGroupStatus != null) {
      return false;
    }
    if (lastRollingOpStatus != null ? !lastRollingOpStatus.equals(response.lastRollingOpStatus)
                                    : response.lastRollingOpStatus != null) {
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
    int result = deploymentGroupResponse != null ? deploymentGroupResponse.hashCode() : 0;
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    result = 31 * result + (hostStatuses != null ? hostStatuses.hashCode() : 0);
    result = 31 * result + (deploymentGroupStatus != null ? deploymentGroupStatus.hashCode() : 0);
    result = 31 * result + (lastRollingOpStatus != null ? lastRollingOpStatus.hashCode() : 0);
    return result;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private DeploymentGroupResponse deploymentGroupResponse;
    private Status status;
    private String error;
    private List<HostStatus> hostStatuses;
    private DeploymentGroupStatus deploymentGroupStatus;
    private RollingOperationStatus lastRollingOpStatus;

    public Builder deploymentGroupResponse(final DeploymentGroupResponse response) {
      this.deploymentGroupResponse = response;
      return this;
    }

    public Builder status(final Status status) {
      this.status = status;
      return this;
    }

    public Builder error(final String error) {
      this.error = error;
      return this;
    }

    public Builder hostStatuses(final List<HostStatus> hostStatuses) {
      this.hostStatuses = hostStatuses;
      return this;
    }

    public Builder deploymentGroupStatus(final DeploymentGroupStatus dgs) {
      this.deploymentGroupStatus = dgs;
      return this;
    }

    public Builder lastRollingOpStatus(final RollingOperationStatus lros) {
      this.lastRollingOpStatus = lros;
      return this;
    }

    public DeploymentGroupStatusResponse build() {
      return new DeploymentGroupStatusResponse(
          this.deploymentGroupResponse,
          this.status,
          this.error,
          this.hostStatuses,
          this.deploymentGroupStatus,
          this.lastRollingOpStatus);
    }
  }
}
