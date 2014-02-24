package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;

import com.aphyr.riemann.Proto.Event;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CapturingRiemannClient extends NoOpRiemannClient {
  private static final ImmutableList<Event> EMPTY_LIST = ImmutableList.<Event>of();
  private AtomicReference<List<Event>> events = new AtomicReference<List<Event>>(EMPTY_LIST);

  public CapturingRiemannClient() {
    super();
    clearEvents();
  }

  public List<Event> getEvents() {
    return events.get();
  }

  public void clearEvents() {
    this.events.set(EMPTY_LIST);
  }

  @Override
  public void sendEvents(final List<Event> events) {
    this.events.set(events);
  }
}
