package com.spotify.helios.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.VersionCompatibility.Status;
import com.spotify.helios.master.PomVersion;

public class VersionCheckResponse {
  private final PomVersion serverVersion;
  private final Status status;
  private final String recommendedVersion;

  public VersionCheckResponse(@JsonProperty("status") VersionCompatibility.Status status,
                              @JsonProperty("version") PomVersion serverVersion,
                              @JsonProperty("recommendedVersion") String recommendedVersion) {
    this.status = status;
    this.serverVersion = serverVersion;
    this.recommendedVersion = recommendedVersion;
  }

  public PomVersion getServerVersion() {
    return serverVersion;
  }

  public Status getStatus() {
    return status;
  }

  public String getRecommendedVersion() {
    return recommendedVersion;
  }
}
