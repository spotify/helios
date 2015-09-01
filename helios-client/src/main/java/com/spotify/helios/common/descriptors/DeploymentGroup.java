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

package com.spotify.helios.common.descriptors;

import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Represents a Helios deployment group.
 *
 * An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "name":"foo-group",
 *   "job":"foo:0.1.0",
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
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeploymentGroup extends Descriptor {

  public static final String EMPTY_NAME = "";
  public static final JobId EMPTY_JOB_ID = null;

  private final String name;
  private final List<HostSelector> hostSelectors;
  private final JobId jobId;
  private final RolloutOptions rolloutOptions;

  /**
   * Create a Job.
   *
   * @param name The docker name to use.
   * @param jobId The job ID for the deployment group.
   * @param hostSelectors The selectors that determine which agents are part of the deployment
   *                       group.
   */
  public DeploymentGroup(
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

    final DeploymentGroup that = (DeploymentGroup) o;

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
    return "DeploymentGroup{" +
           "name='" + name + '\'' +
           ", hostSelectors=" + hostSelectors +
           ", job=" + jobId +
           ", rolloutOptions=" + rolloutOptions +
           '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    return builder.setName(name)
        .setJobId(jobId)
        .setHostSelectors(hostSelectors)
        .setRolloutOptions(rolloutOptions);
  }

  public static class Builder implements Cloneable {

    private final Parameters p;

    private Builder() {
      this.p = new Parameters();
    }

    private static class Parameters implements Cloneable {

      public String name;
      public JobId jobId;
      public List<HostSelector> hostSelectors;
      public RolloutOptions rolloutOptions;

      private Parameters() {
        this.name = EMPTY_NAME;
        this.jobId = EMPTY_JOB_ID;
        this.hostSelectors = emptyList();
        this.rolloutOptions = null;
      }
    }

    public String getName() {
      return p.name;
    }

    public Builder setName(final String name) {
      p.name = name;
      return this;
    }

    public JobId getJobId() {
      return p.jobId;
    }

    public Builder setJobId(final JobId jobId) {
      p.jobId = jobId;
      return this;
    }

    public List<HostSelector> getHostSelectors() {
      return p.hostSelectors;
    }

    public Builder setHostSelectors(final List<HostSelector> hostSelectors) {
      p.hostSelectors = Lists.newArrayList(hostSelectors);
      return this;
    }

    public RolloutOptions getRolloutOptions() {
      return p.rolloutOptions;
    }

    public Builder setRolloutOptions(final RolloutOptions rolloutOptions) {
      p.rolloutOptions = rolloutOptions;
      return this;
    }

    public DeploymentGroup build() {
      return new DeploymentGroup(p.name, p.hostSelectors, p.jobId, p.rolloutOptions);
    }
  }

}
