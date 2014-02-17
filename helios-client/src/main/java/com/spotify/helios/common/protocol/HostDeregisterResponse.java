package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HostDeregisterResponse {

  public enum Status {OK, NOT_FOUND, JOBS_STILL_DEPLOYED}

  private final Status status;
  private final String host;

  public HostDeregisterResponse(@JsonProperty("status") Status status,
                                @JsonProperty("host") String host) {
    this.status = status;
    this.host = host;
  }

  public Status getStatus() {
    return status;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("HostDeregisterResponse")
        .add("status", status)
        .add("host", host)
        .toString();
  }
}
