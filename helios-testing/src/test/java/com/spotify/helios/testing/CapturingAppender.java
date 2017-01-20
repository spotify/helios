/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
