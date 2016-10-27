package com.spotify.helios.testing;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.collect.ImmutableList;

final class CapturingAppender extends AppenderBase<ILoggingEvent> {

  private final ImmutableList.Builder<ILoggingEvent> eventsBuilder;

  private CapturingAppender(final ImmutableList.Builder<ILoggingEvent> eventsBuilder) {
    this.eventsBuilder = eventsBuilder;
  }

  public static CapturingAppender create() {
    return new CapturingAppender(ImmutableList.<ILoggingEvent>builder());
  }

  @Override
  protected void append(final ILoggingEvent eventObject) {
    eventsBuilder.add(eventObject);
  }

  public ImmutableList<ILoggingEvent> events() {
    return eventsBuilder.build();
  }
}
