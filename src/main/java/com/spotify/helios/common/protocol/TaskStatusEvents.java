package com.spotify.helios.common.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TaskStatusEvents {
  private final List<TaskStatusEvent> events;

  public TaskStatusEvents(@JsonProperty("events") List<TaskStatusEvent> events) {
    this.events = events;
  }

  public List<TaskStatusEvent> getEvents() {
    return events;
  }
}
