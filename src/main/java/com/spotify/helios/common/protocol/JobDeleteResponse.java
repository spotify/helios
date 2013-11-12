package com.spotify.helios.common.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class JobDeleteResponse {
  public enum Status { OK, STILL_IN_USE }

  private final Status status;

  public JobDeleteResponse(@JsonProperty("status") Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(JobDeleteResponse.class)
        .add("status", status)
        .toString();
  }
}
