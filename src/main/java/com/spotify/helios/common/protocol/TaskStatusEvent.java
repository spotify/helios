package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.descriptors.TaskStatus;

public class TaskStatusEvent {
  private final TaskStatus status;
  private final long timestamp;
  private final String agent;

  public TaskStatusEvent(@JsonProperty("status") final TaskStatus status,
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

  @Override
  public String toString() {
    return Objects.toStringHelper(TaskStatusEvent.class)
        .add("timestamp", timestamp)
        .add("agent", agent)
        .add("status", status)
        .toString();
  }
}
