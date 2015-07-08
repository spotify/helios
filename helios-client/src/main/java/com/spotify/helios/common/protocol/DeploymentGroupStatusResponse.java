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
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class DeploymentGroupStatusResponse {

  public enum Status {
    ROLLING_OUT,
    ACTIVE,
    FAILED
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

  private final String name;
  private final Status status;
  private final String error;
  private final JobId jobId;
  private final List<HostStatus> hostStatuses;
  private final DeploymentGroupStatus deploymentGroupStatus;

  public DeploymentGroupStatusResponse(
      @JsonProperty("name") final String name,
      @JsonProperty("status") final Status status,
      @JsonProperty("jobId") final JobId jobId,
      @JsonProperty("error") final String error,
      @JsonProperty("hostStatuses") final List<HostStatus> hostStatuses,
      @JsonProperty("deploymentGroupStatus") final DeploymentGroupStatus deploymentGroupStatus) {
    this.name = name;
    this.status = status;
    this.error = error;
    this.jobId = jobId;
    this.hostStatuses = hostStatuses;
    this.deploymentGroupStatus = deploymentGroupStatus;
  }

  public String getName() {
    return name;
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

  public JobId getJobId() {
    return jobId;
  }

  public DeploymentGroupStatus getDeploymentGroupStatus() {
    return deploymentGroupStatus;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("name", name)
        .add("status", status)
        .add("error", error)
        .add("jobId", jobId)
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

    final DeploymentGroupStatusResponse that = (DeploymentGroupStatusResponse) o;

    if (deploymentGroupStatus != null ? !deploymentGroupStatus.equals(that.deploymentGroupStatus)
                                      : that.deploymentGroupStatus != null) {
      return false;
    }
    if (error != null ? !error.equals(that.error) : that.error != null) {
      return false;
    }
    if (hostStatuses != null ? !hostStatuses.equals(that.hostStatuses)
                             : that.hostStatuses != null) {
      return false;
    }
    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (status != that.status) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
    result = 31 * result + (hostStatuses != null ? hostStatuses.hashCode() : 0);
    result = 31 * result + (deploymentGroupStatus != null ? deploymentGroupStatus.hashCode() : 0);
    return result;
  }
}
