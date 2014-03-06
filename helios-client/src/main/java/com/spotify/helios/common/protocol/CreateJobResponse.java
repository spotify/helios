package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CreateJobResponse {

  public enum Status {
    OK,
    ID_MISMATCH,
    JOB_ALREADY_EXISTS,
    INVALID_JOB_DEFINITION,
  }

  private final Status status;
  private final List<String> errors;
  private final String id;

  public CreateJobResponse(@JsonProperty("status") final Status status,
                           @JsonProperty("errors") final List<String> errors,
                           @JsonProperty("id") final String id) {
    this.status = status;
    this.errors = errors;
    this.id = id;
  }

  public Status getStatus() {
    return status;
  }

  public List<String> getErrors() {
    return errors;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("JobDeployResponse")
        .add("status", status)
        .toString();
  }
}
