package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateJobResponse {

  public enum Status {
    OK,
    ID_MISMATCH,
    JOB_ALREADY_EXISTS
  }

  private final Status status;

  public CreateJobResponse(@JsonProperty("status") Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("JobDeployResponse")
        .add("status", status)
        .toString();
  }
}
