package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;

import com.aphyr.riemann.Proto.Event;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CapturingRiemannClient extends NoOpRiemannClient {
  private static final ImmutableList<Event> EMPTY_LIST = ImmutableList.<Event>of();

  private final AtomicReference<List<Event>> events;

  public CapturingRiemannClient() {
    super();
    this.events = new AtomicReference<List<Event>>(EMPTY_LIST);
  }

  public synchronized List<Event> getEvents() {
    final List<Event> returnVal = events.get();
    clearEvents();
    return returnVal;
  }

  public void clearEvents() {
    this.events.set(EMPTY_LIST);
  }

  @Override
  public void sendEvents(final List<Event> events) {
    this.events.set(events);
  }
}
