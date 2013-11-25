package com.spotify.helios.common.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.descriptors.TaskStatus;

public class JobStatusEvent {
  private final TaskStatus status;
  private final long timestamp;
  private final String agent;

  public JobStatusEvent(@JsonProperty("status") final TaskStatus status,
                        @JsonProperty("timestamp") final long timestamp,
                        @JsonProperty("agent") final String agent) {
    this.status = status;
    this.timestamp = timestamp;
    this.agent = agent;
  }

  public String getAgent() {
    return agent;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
