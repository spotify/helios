package com.spotify.helios.common.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class JobStatusEvents {
  private final List<JobStatusEvent> events;

  public JobStatusEvents(@JsonProperty("events") List<JobStatusEvent> events) {
    this.events = events;
  }

  public List<JobStatusEvent> getEvents() {
    return events;
  }
}
