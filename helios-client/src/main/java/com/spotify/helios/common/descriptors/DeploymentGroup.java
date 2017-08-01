/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.common.descriptors;

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a Helios deployment group.
 *
 * <p>An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "name":"foo-group",
 *   "jobId":"foo:0.1.0",
 *   "hostSelectors":[
 *     {
 *       "label":"foo",
 *       "operator":"EQUALS"
 *       "operand":"bar",
 *     },
 *     {
 *       "label":"baz",
 *       "operator":"EQUALS"
 *       "operand":"qux",
 *     }
 *   ],
 *   "rolloutOptions":{
 *     "migrate":false,
 *     "parallelism":2,
 *     "timeout":1000,
 *     "overlap":true,
 *     "token": "insecure-access-token"
 *   },
 *   "rollingUpdateReason": "MANUAL"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeploymentGroup extends Descriptor {

  public enum RollingUpdateReason {
    MANUAL,
    HOSTS_CHANGED
  }

  public static final String EMPTY_NAME = "";
  public static final JobId EMPTY_JOB_ID = null;

  private final String name;
  private final List<HostSelector> hostSelectors;
  private final JobId jobId;
  private final RolloutOptions rolloutOptions;
  private final RollingUpdateReason reason;

  /**
   * Create a Job.
   *
   * <p>Note that despite annotating jobId as @JsonProperty("job") in practice it's serialised as
   * "jobId" because we neglected to annotate the getter to match. The annotation has been left here
   * to ensure backwards compatibility.
   *
   * @param name          The docker name to use.
   * @param jobId         The job ID for the deployment group.
   * @param hostSelectors The selectors that determine which agents are part of the deployment
   *                      group.
   * @param reason        The reason the most recent rolling update (if any) occurred.
   */
  public DeploymentGroup(
      @JsonProperty("name") final String name,
      @JsonProperty("hostSelectors") final List<HostSelector> hostSelectors,
      @JsonProperty("job") @Nullable final JobId jobId,
      @JsonProperty("rolloutOptions") @Nullable final RolloutOptions rolloutOptions,
      @JsonProperty("rollingUpdateReason") @Nullable final RollingUpdateReason reason) {
    this.name = name;
    this.hostSelectors = hostSelectors;
    this.jobId = jobId;
    this.rolloutOptions = rolloutOptions;
    this.reason = reason;
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

  public RollingUpdateReason getRollingUpdateReason() {
    return reason;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final DeploymentGroup that = (DeploymentGroup) obj;

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

    if (reason != null ? !reason.equals(that.reason) : that.reason != null) {
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
    result = 31 * result + (reason != null ? reason.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentGroup{"
           + "name='" + name + '\''
           + ", hostSelectors=" + hostSelectors
           + ", job=" + jobId
           + ", rolloutOptions=" + rolloutOptions
           + ", reason=" + reason
           + '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    return builder.setName(name)
        .setJobId(jobId)
        .setHostSelectors(hostSelectors)
        .setRolloutOptions(rolloutOptions)
        .setRollingUpdateReason(reason);
  }

  public static class Builder implements Cloneable {

    private final Parameters pm;

    private Builder() {
      this.pm = new Parameters();
    }

    private static class Parameters implements Cloneable {

      public String name;
      public JobId jobId;
      public List<HostSelector> hostSelectors;
      public RolloutOptions rolloutOptions;
      public RollingUpdateReason reason;

      private Parameters() {
        this.name = EMPTY_NAME;
        this.jobId = EMPTY_JOB_ID;
        this.hostSelectors = emptyList();
        this.rolloutOptions = null;
        this.reason = null;
      }
    }

    public String getName() {
      return pm.name;
    }

    public Builder setName(final String name) {
      pm.name = name;
      return this;
    }

    public JobId getJobId() {
      return pm.jobId;
    }

    public Builder setJobId(final JobId jobId) {
      pm.jobId = jobId;
      return this;
    }

    public List<HostSelector> getHostSelectors() {
      return pm.hostSelectors;
    }

    public Builder setHostSelectors(final List<HostSelector> hostSelectors) {
      pm.hostSelectors = Lists.newArrayList(hostSelectors);
      return this;
    }

    public RolloutOptions getRolloutOptions() {
      return pm.rolloutOptions;
    }

    public Builder setRolloutOptions(final RolloutOptions rolloutOptions) {
      pm.rolloutOptions = rolloutOptions;
      return this;
    }

    public RollingUpdateReason getRollingUpdateReason() {
      return pm.reason;
    }

    public Builder setRollingUpdateReason(final RollingUpdateReason reason) {
      pm.reason = reason;
      return this;
    }

    public DeploymentGroup build() {
      return new DeploymentGroup(pm.name, pm.hostSelectors, pm.jobId, pm.rolloutOptions, pm.reason);
    }
  }

}
