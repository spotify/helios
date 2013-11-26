package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TaskStatusEvents {
  public enum Status {OK, JOB_ID_NOT_FOUND}

  private final List<TaskStatusEvent> events;
  private final Status status;

  public TaskStatusEvents(@JsonProperty("events") List<TaskStatusEvent> events,
                          @JsonProperty("status") Status status) {
    this.events = events;
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public List<TaskStatusEvent> getEvents() {
    return events;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(TaskStatusEvents.class)
        .add("status", status)
        .add("events", events)
        .toString();
  }
}
