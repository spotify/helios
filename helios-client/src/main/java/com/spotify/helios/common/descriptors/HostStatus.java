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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;

public class HostStatus extends Descriptor {

  public static enum Status {
    UP,
    DOWN
  }

  private final Status status;
  private final HostInfo hostInfo;
  private final AgentInfo agentInfo;
  private final Map<JobId, Deployment> jobs;
  private final Map<JobId, TaskStatus> statuses;
  private final Map<String, String> environment;

  public HostStatus(@JsonProperty("jobs") final Map<JobId, Deployment> jobs,
                    @JsonProperty("statuses") final Map<JobId, TaskStatus> statuses,
                    @JsonProperty("status") final Status status,
                    @JsonProperty("hostInfo") final HostInfo hostInfo,
                    @JsonProperty("agentInfo") final AgentInfo agentInfo,
                    @JsonProperty("environment") final Map<String, String> environment) {
    this.status = checkNotNull(status, "status");
    this.jobs = checkNotNull(jobs, "jobs");
    this.statuses = checkNotNull(statuses, "statuses");

    // Host, runtime info and environment might not be available
    this.hostInfo = hostInfo;
    this.agentInfo = agentInfo;
    this.environment = fromNullable(environment).or(Collections.<String, String>emptyMap());
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public Status getStatus() {
    return status;
  }

  @Nullable
  public HostInfo getHostInfo() {
    return hostInfo;
  }

  @Nullable
  public AgentInfo getAgentInfo() {
    return agentInfo;
  }

  public Map<JobId, Deployment> getJobs() {
    return jobs;
  }

  public Map<JobId, TaskStatus> getStatuses() {
    return statuses;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Map<JobId, Deployment> jobs;
    private Map<JobId, TaskStatus> statuses;
    private Status status;
    private HostInfo hostInfo;
    private AgentInfo agentInfo;
    private Map<String, String> environment;

    public Builder setJobs(final Map<JobId, Deployment> jobs) {
      this.jobs = jobs;
      return this;
    }

    public Builder setStatuses(final Map<JobId, TaskStatus> statuses) {
      this.statuses = statuses;
      return this;
    }

    public Builder setStatus(final Status status) {
      this.status = status;
      return this;
    }

    public Builder setHostInfo(final HostInfo hostInfo) {
      this.hostInfo = hostInfo;
      return this;
    }

    public Builder setAgentInfo(final AgentInfo agentInfo) {
      this.agentInfo = agentInfo;
      return this;
    }

    public Builder setEnvironment(final Map<String, String> environment) {
      this.environment = environment;
      return this;
    }

    public HostStatus build() {
      return new HostStatus(jobs, statuses, status, hostInfo, agentInfo, environment);
    }
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

    if (hostInfo != null ? !hostInfo.equals(that.hostInfo) : that.hostInfo != null) {
      return false;
    }
    if (jobs != null ? !jobs.equals(that.jobs) : that.jobs != null) {
      return false;
    }
    if (agentInfo != null ? !agentInfo.equals(that.agentInfo) : that.agentInfo != null) {
      return false;
    }
    if (status != that.status) {
      return false;
    }
    if (statuses != null ? !statuses.equals(that.statuses) : that.statuses != null) {
      return false;
    }
    if (environment != null ? !environment.equals(that.environment) : that.environment != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = status != null ? status.hashCode() : 0;
    result = 31 * result + (hostInfo != null ? hostInfo.hashCode() : 0);
    result = 31 * result + (agentInfo != null ? agentInfo.hashCode() : 0);
    result = 31 * result + (jobs != null ? jobs.hashCode() : 0);
    result = 31 * result + (statuses != null ? statuses.hashCode() : 0);
    result = 31 * result + (environment != null ? environment.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    final String strEnv = Joiner.on(", ").join(
        Iterables.transform(environment.entrySet(), new Function<Entry<String, String>, String>() {
          @Override
          public String apply(Entry<String, String> entry) {
            return entry.getKey() + "=" + entry.getValue();
          }
        }));
    return "HostStatus{" +
           "status=" + status +
           ", hostInfo=" + hostInfo +
           ", agentInfo=" + agentInfo +
           ", jobs=" + jobs +
           ", statuses=" + statuses +
           ", environment={" + strEnv + "}" +
           '}';
  }
}
