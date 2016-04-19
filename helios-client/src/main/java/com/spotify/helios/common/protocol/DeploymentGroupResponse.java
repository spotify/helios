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
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;

import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * A DeploymentGroupResponse is a shim to maintain the appearance that a DeploymentGroup has a
 * job ID and rollout operations per the legacy API, despite the fact that these options are now
 * associated with rolling operations (which are in turn associated with the DeploymentGroup). A
 * DeploymentGroupResponse can be created from the DeploymentGroup in question and its last good
 * rolling operation.
 */
public class DeploymentGroupResponse {

  private final String name;
  private final List<HostSelector> hostSelectors;
  private final JobId jobId;
  private final RolloutOptions rolloutOptions;

  public DeploymentGroupResponse(
      @JsonProperty("name") final String name,
      @JsonProperty("hostSelectors") final List<HostSelector> hostSelectors,
      @JsonProperty("job") @Nullable final JobId jobId,
      @JsonProperty("rolloutOptions") @Nullable final RolloutOptions rolloutOptions) {
    this.name = name;
    this.hostSelectors = hostSelectors;
    this.jobId = jobId;
    this.rolloutOptions = rolloutOptions;
  }

  public String getName() {
    return name;
  }

  public JobId getJobId() {
    return jobId;
  }

  public List<HostSelector> getHostSelectors() {
    return hostSelectors;
  }

  public RolloutOptions getRolloutOptions() {
    return rolloutOptions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DeploymentGroupResponse that = (DeploymentGroupResponse) o;

    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (hostSelectors != null ? !hostSelectors.equals(that.hostSelectors)
                              : that.hostSelectors != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (rolloutOptions != null ? !rolloutOptions.equals(that.rolloutOptions)
                               : that.rolloutOptions != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (hostSelectors != null ? hostSelectors.hashCode() : 0);
    result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
    result = 31 * result + (rolloutOptions != null ? rolloutOptions.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentGroupResponse{" +
           "name='" + name + '\'' +
           ", hostSelectors=" + hostSelectors +
           ", job=" + jobId +
           ", rolloutOptions=" + rolloutOptions +
           '}';
  }

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private List<HostSelector> hostSelectors;
    private JobId jobId;
    private RolloutOptions rolloutOptions;

    public Builder name(final String name) {
      this.name = name;
      return this;
    }

    public Builder hostSelectors(final List<HostSelector> hostSelectors) {
      this.hostSelectors = hostSelectors;
      return this;
    }

    public Builder jobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder rolloutOptions(final RolloutOptions rolloutOptions) {
      this.rolloutOptions = rolloutOptions;
      return this;
    }

    public DeploymentGroupResponse build() {
      return new DeploymentGroupResponse(
          this.name, this.hostSelectors, this.jobId, this.rolloutOptions);
    }
  }

}
