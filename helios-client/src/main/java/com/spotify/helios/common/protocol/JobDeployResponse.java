package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobDeployResponse {

  public enum Status {
    OK,
    JOB_NOT_FOUND,
    PORT_CONFLICT,
    AGENT_NOT_FOUND,
    JOB_ALREADY_DEPLOYED,
    ID_MISMATCH,
    INVALID_ID
  }

  private final Status status;
  private final String job;
  private final String agent;

  public JobDeployResponse(@JsonProperty("status") Status status,
                           @JsonProperty("agent") String agent,
                           @JsonProperty("job") String job) {
    this.status = status;
    this.job = job;
    this.agent = agent;
  }

  public Status getStatus() {
    return status;
  }

  public String getJob() {
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
