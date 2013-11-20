/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class AgentStatus extends Descriptor {

  public static enum Status {
    UP,
    DOWN
  }

  private final Status status;
  private final HostInfo hostInfo;
  private final Map<JobId, Deployment> jobs;
  private final Map<JobId, TaskStatus> statuses;

  public AgentStatus(@JsonProperty("jobs") final Map<JobId, Deployment> jobs,
                     @JsonProperty("statuses") final Map<JobId, TaskStatus> statuses,
                     @JsonProperty("status") final Status status,
                     @JsonProperty("hostInfo") final HostInfo hostInfo) {
    this.status = checkNotNull(status);
    this.jobs = checkNotNull(jobs);
    this.statuses = checkNotNull(statuses);

    // Host info might not be available
    this.hostInfo = hostInfo;
  }

  public Status getStatus() {
    return status;
  }

  public HostInfo getHostInfo() {
    return hostInfo;
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

    public AgentStatus build() {
      return new AgentStatus(jobs, statuses, status, hostInfo);
    }
  }

  @Override
  public String toString() {
    return "AgentStatus{" +
           "status=" + status +
           ", hostInfo=" + hostInfo +
           ", jobs=" + jobs +
           ", statuses=" + statuses +
           '}';
  }
}
