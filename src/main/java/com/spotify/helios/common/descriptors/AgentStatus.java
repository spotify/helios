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
  private final Map<String, AgentJob> jobs;
  private final Map<String, JobStatus> statuses;

  public AgentStatus(@JsonProperty("jobs") final Map<String, AgentJob> jobs,
                     @JsonProperty("statuses") final Map<String, JobStatus> statuses,
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

  public Map<String, AgentJob> getJobs() {
    return jobs;
  }

  public Map<String, JobStatus> getStatuses() {
    return statuses;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Map<String, AgentJob> jobs;
    private Map<String, JobStatus> statuses;
    private Status status;
    private HostInfo hostInfo;

    public Builder setJobs(final Map<String, AgentJob> jobs) {
      this.jobs = jobs;
      return this;
    }

    public Builder setStatuses(final Map<String, JobStatus> statuses) {
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
