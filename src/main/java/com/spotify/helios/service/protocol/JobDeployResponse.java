package com.spotify.helios.service.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class JobDeployResponse {
  public enum Status { OK, JOB_NOT_FOUND, AGENT_NOT_FOUND };
  private final Status status;
  private final String message;

  public JobDeployResponse(@JsonProperty("status") Status status,
                           @JsonProperty("message") String message) {
    this.status = status;
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("JobDeployResponse")
        .add("status", status)
        .add("message", message)
        .toString();
  }
}
