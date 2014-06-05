/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.IPromise;

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
  public IPromise<Boolean> aSendEventsWithAck(final List<Event> events) {
    this.events.set(events);
    return super.aSendEventsWithAck(events);
  }
}
