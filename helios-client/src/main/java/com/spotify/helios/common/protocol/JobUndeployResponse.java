package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.descriptors.JobId;

public class JobUndeployResponse {

  public enum Status {
    OK,
    JOB_NOT_FOUND,
    AGENT_NOT_FOUND,
    INVALID_ID
  }

  private final Status status;
  private final JobId job;
  private final String agent;

  public JobUndeployResponse(@JsonProperty("status") Status status,
                             @JsonProperty("agent") String agent,
                             @JsonProperty("job") JobId job) {
    this.status = status;
    this.job = job;
    this.agent = agent;
  }

  public Status getStatus() {
    return status;
  }

  public JobId getJob() {
    return job;
  }

  public String getAgent() {
    return agent;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("JobDeployResponse")
        .add("status", status)
        .add("agent", agent)
        .add("job", job)
        .toString();
  }
}
