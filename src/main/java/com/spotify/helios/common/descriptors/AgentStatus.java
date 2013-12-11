/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class AgentStatus extends Descriptor {

  public static enum Status {
    UP,
    DOWN
  }

  private final Status status;
  private final HostInfo hostInfo;
  private final RuntimeInfo runtimeInfo;
  private final Map<JobId, Deployment> jobs;
  private final Map<JobId, TaskStatus> statuses;
  private final Map<String, String> environment;

  public AgentStatus(@JsonProperty("jobs") final Map<JobId, Deployment> jobs,
                     @JsonProperty("statuses") final Map<JobId, TaskStatus> statuses,
                     @JsonProperty("status") final Status status,
                     @JsonProperty("hostInfo") final HostInfo hostInfo,
                     @JsonProperty("runtimeInfo") final RuntimeInfo runtimeInfo,
                     @JsonProperty("environment") final Map<String, String> environment) {
    this.status = checkNotNull(status, "status");
    this.jobs = checkNotNull(jobs, "jobs");
    this.statuses = checkNotNull(statuses, "statuses");
    this.environment = checkNotNull(environment, "environment");

    // Host and runtime info might not be available
    this.hostInfo = hostInfo;
    this.runtimeInfo = runtimeInfo;
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
  public RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
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
    private RuntimeInfo runtimeInfo;
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

    public Builder setRuntimeInfo(final RuntimeInfo runtimeInfo) {
      this.runtimeInfo = runtimeInfo;
      return this;
    }

    public Builder setEnvironment(final Map<String, String> environment) {
      this.environment = environment;
      return this;
    }

    public AgentStatus build() {
      return new AgentStatus(jobs, statuses, status, hostInfo, runtimeInfo, environment);
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

    final AgentStatus that = (AgentStatus) o;

    if (hostInfo != null ? !hostInfo.equals(that.hostInfo) : that.hostInfo != null) {
      return false;
    }
    if (jobs != null ? !jobs.equals(that.jobs) : that.jobs != null) {
      return false;
    }
    if (runtimeInfo != null ? !runtimeInfo.equals(that.runtimeInfo) : that.runtimeInfo != null) {
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
    result = 31 * result + (runtimeInfo != null ? runtimeInfo.hashCode() : 0);
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
    return "AgentStatus{" +
           "status=" + status +
           ", hostInfo=" + hostInfo +
           ", runtimeInfo=" + runtimeInfo +
           ", jobs=" + jobs +
           ", statuses=" + statuses +
           ", environment={" + strEnv + "}" +
           '}';
  }
}
