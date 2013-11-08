/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class AgentStatus extends Descriptor {

  private final Map<String, AgentJob> jobs;
  private final Map<String, JobStatus> statuses;

  public AgentStatus(@JsonProperty("jobs") final Map<String, AgentJob> jobs,
                     @JsonProperty("statuses") final Map<String, JobStatus> statuses) {
    this.jobs = checkNotNull(jobs);
    this.statuses = checkNotNull(statuses);
  }

  public Map<String, AgentJob> getJobs() {
    return jobs;
  }

  public Map<String, JobStatus> getStatuses() {
    return statuses;
  }
}
