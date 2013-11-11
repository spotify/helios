package com.spotify.helios.common.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class AgentDeleteResponse {
  public enum Status {OK, NOT_FOUND}
  private final Status status;
  private final String agent;

  public AgentDeleteResponse(@JsonProperty("status") Status status,
                             @JsonProperty("agent") String agent) {
    this.status = status;
    this.agent = agent;
  }

  public Status getStatus() {
    return status;
  }

  public String getAgent() {
    return agent;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("AgentDeleteResponse")
        .add("status", status)
        .add("agent", agent)
        .toString();
  }
}
