package com.spotify.helios.service.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class JobUndeployResponse {

  public enum Status { OK, JOB_NOT_FOUND, AGENT_NOT_FOUND };
  private final Status status;
  private final String job;
  private final String agent;

  public JobUndeployResponse(@JsonProperty("status") Status status,
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
