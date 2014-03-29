package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskStatusEvent {
  private final TaskStatus status;
  private final long timestamp;
  private final String host;

  public TaskStatusEvent(@JsonProperty("status") final TaskStatus status,
                         @JsonProperty("timestamp") final long timestamp,
                         @JsonProperty("host") final String host) {
    this.status = status;
    this.timestamp = timestamp;
    this.host = host;
  }

  public String getHost() {
    return host;
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
        .add("host", host)
        .add("status", status)
        .toString();
  }
}
