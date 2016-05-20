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

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Represents a rolling operation on a Helios deployment group.
 *
 * An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "id":"some-cool-uuid",
 *   "deploymentGroupName":"my-awesome-group",
 *   "job":"foo:0.1.0",
 *   "reason":"HOSTS_CHANGED",
 *   "rolloutOptions":{
 *     "migrate":false,
 *     "parallelism":2,
 *     "timeout":1000,
 *     "overlap":true,
 *     "token": "insecure-access-token"
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RollingOperation extends Descriptor {

  public enum Reason {
    MANUAL,
    HOSTS_CHANGED,
  }

  private static final String EMPTY_DEPLOYMENT_GROUP_NAME = "";
  private static final JobId EMPTY_JOB_ID = null;

  private final String id;
  private final String deploymentGroupName;
  private final JobId jobId;
  private final Reason reason;
  private final RolloutOptions rolloutOptions;

  /**
   * Create a rolling operation.
   *
   * @param id The docker id to use.
   * @param jobId The job ID for the deployment group.
   */
  public RollingOperation(
      @JsonProperty("id") final String id,
      @JsonProperty("deploymentGroupName") final String deploymentGroupName,
      @JsonProperty("reason") final Reason reason,
      @JsonProperty("job") @Nullable final JobId jobId,
      @JsonProperty("rolloutOptions") @Nullable final RolloutOptions rolloutOptions) {
    this.id = id;
    this.deploymentGroupName = deploymentGroupName;
    this.jobId = jobId;
    this.reason = reason;
    this.rolloutOptions = rolloutOptions;
  }

  public String getId() {
    return id;
  }

  public String getDeploymentGroupName() {
    return deploymentGroupName;
  }

  public JobId getJobId() {
    return jobId;
  }

  public Reason getReason() {
    return reason;
  }

  public RolloutOptions getRolloutOptions() {
    return rolloutOptions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RollingOperation that = (RollingOperation) o;

    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (deploymentGroupName != null ? !deploymentGroupName.equals(that.deploymentGroupName)
                                    : that.deploymentGroupName != null) {
      return false;
    }
    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (reason != null ? !reason.equals(that.reason) : that.reason != null) {
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
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (deploymentGroupName != null ? deploymentGroupName.hashCode() : 0);
    result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
    result = 31 * result + (reason != null ? reason.hashCode() : 0);
    result = 31 * result + (rolloutOptions != null ? rolloutOptions.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RollingOperation{" +
           "id='" + id + '\'' +
           ", deploymentGroupName=" + deploymentGroupName +
           ", job=" + jobId +
           ", reason=" + reason +
           ", rolloutOptions=" + rolloutOptions +
           '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    return builder
        .setJobId(jobId)
        .setDeploymentGroupName(deploymentGroupName)
        .setReason(reason)
        .setRolloutOptions(rolloutOptions);
  }

  public static class Builder {

    private String id = UUID.randomUUID().toString();
    private String deploymentGroupName = EMPTY_DEPLOYMENT_GROUP_NAME;
    private JobId jobId = EMPTY_JOB_ID;
    private Reason reason = Reason.MANUAL;
    private RolloutOptions rolloutOptions;

    public String getId() {
      return this.id;
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public String getDeploymentGroupName() {
      return this.deploymentGroupName;
    }

    public Builder setDeploymentGroupName(final String deploymentGroupName) {
      this.deploymentGroupName = deploymentGroupName;
      return this;
    }

    public JobId getJobId() {
      return this.jobId;
    }

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Reason getReason() {
      return this.reason;
    }

    public Builder setReason(final Reason reason) {
      this.reason = reason;
      return this;
    }

    public RolloutOptions getRolloutOptions() {
      return this.rolloutOptions;
    }

    public Builder setRolloutOptions(final RolloutOptions rolloutOptions) {
      this.rolloutOptions = rolloutOptions;
      return this;
    }

    public RollingOperation build() {
      return new RollingOperation(
          this.id, this.deploymentGroupName, this.reason, this.jobId, this.rolloutOptions);
    }
  }

}
