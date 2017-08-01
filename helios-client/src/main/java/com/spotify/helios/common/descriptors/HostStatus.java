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

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Gives the Helios host status for the agent, which includes all jobs, their statuses, as well
 * as host and agent information.
 *
 * <pre>
 * {
 *   "agentInfo" : { #... see the description of AgentInfo },
 *   "environment" : {
 *     "SYSLOG_HOST_PORT" : "10.99.0.1:514",
 *   },
 *   "hostInfo" : { #... see the description of HostInfo },
 *   "jobs" : {
 *     "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265" : {
 *       "goal" : "START",
 *       "jobId" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265",
 *     }
 *   },
 *   "labels" : {
 *     "role" : "foo",
 *     "xyz" : "123"
 *   },
 *   "status" : "UP",
 *   "statuses" : {
 *     "elva:0.0.4:9f64cf43353c55c36276b7df76b066584f9c49aa" : {
 *       "containerId" : "5a31d4fd48b5b4349980175e2f865494146704e684d89b6a95a9a766cc2f43a3",
 *       "env" : {
 *         "SYSLOG_HOST_PORT" : "10.99.0.1:514",
 *       },
 *       "goal" : "START",
 *       "job" : { #... See definition of Job },
 *       "state" : "RUNNING",
 *       "throttled" : "NO"
 *     }
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HostStatus extends Descriptor {

  public enum Status {
    UP,
    DOWN
  }

  private final Status status;
  private final HostInfo hostInfo;
  private final AgentInfo agentInfo;
  private final Map<JobId, Deployment> jobs;
  private final Map<JobId, TaskStatus> statuses;
  private final Map<String, String> environment;
  private final Map<String, String> labels;

  /**
   * Constructor.
   *
   * @param jobs        Map of jobs and their deployments for this host.
   * @param statuses    the statuses of jobs on this host.
   * @param status      The up/down status of this host.
   * @param hostInfo    The host information.
   * @param agentInfo   The agent information.
   * @param environment The environment provided to the agent on its command line.
   * @param labels      The labels assigned to the agent.
   */
  public HostStatus(@JsonProperty("jobs") final Map<JobId, Deployment> jobs,
                    @JsonProperty("statuses") final Map<JobId, TaskStatus> statuses,
                    @JsonProperty("status") final Status status,
                    @JsonProperty("hostInfo") final HostInfo hostInfo,
                    @JsonProperty("agentInfo") final AgentInfo agentInfo,
                    @JsonProperty("environment") final Map<String, String> environment,
                    @JsonProperty("labels") final Map<String, String> labels) {
    this.status = checkNotNull(status, "status");
    this.jobs = checkNotNull(jobs, "jobs");
    this.statuses = checkNotNull(statuses, "statuses");

    // Host, runtime info and environment might not be available
    this.hostInfo = hostInfo;
    this.agentInfo = agentInfo;
    this.environment = fromNullable(environment).or(Collections.<String, String>emptyMap());
    this.labels = fromNullable(labels).or(Collections.<String, String>emptyMap());
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public Map<String, String> getLabels() {
    return labels;
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
    private Map<String, String> labels;

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

    public Builder setLabels(final Map<String, String> labels) {
      this.labels = labels;
      return this;
    }

    public HostStatus build() {
      return new HostStatus(jobs, statuses, status, hostInfo, agentInfo, environment, labels);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final HostStatus that = (HostStatus) obj;

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
    if (labels != null ? !labels.equals(that.labels) : that.labels != null) {
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
    result = 31 * result + (labels != null ? labels.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "HostStatus{"
           + "status=" + status
           + ", hostInfo=" + hostInfo
           + ", agentInfo=" + agentInfo
           + ", jobs=" + jobs
           + ", statuses=" + statuses
           + ", environment=" + stringMapToString(environment)
           + ", labels=" + stringMapToString(labels)
           + '}';
  }

  private static String stringMapToString(final Map<String, String> map) {
    return "{" + Joiner.on(", ").withKeyValueSeparator("=").join(map) + "}";
  }
}
