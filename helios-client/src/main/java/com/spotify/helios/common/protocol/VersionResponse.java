package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VersionResponse {

  private final String clientVersion;
  private final String masterVersion;

  public VersionResponse(@JsonProperty("clientVersion") String clientVersion,
                         @JsonProperty("masterVersion") String masterVersion) {
    this.clientVersion = clientVersion;
    this.masterVersion = masterVersion;
  }

  public String getClientVersion() {
    return clientVersion;
  }

  public String getMasterVersion() {
    return masterVersion;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(VersionResponse.class)
        .add("clientVersion", clientVersion)
        .add("masterVersion", masterVersion)
        .toString();
  }
}
