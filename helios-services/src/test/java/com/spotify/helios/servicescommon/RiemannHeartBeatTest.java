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

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.Polling;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RiemannHeartBeatTest {

  @Test
  public void test() throws Exception {
    final CapturingRiemannClient client = new CapturingRiemannClient();

    final RiemannHeartBeat hb = new RiemannHeartBeat(TimeUnit.MILLISECONDS, 1, client.facade());
    hb.start();

    final List<Event> events = Polling.await(10, TimeUnit.SECONDS, new Callable<List<Event>>() {
      @Override
      public List<Event> call() throws Exception {
        final List<Event> events = client.getEvents();
        return !events.isEmpty() ? events : null;
      }
    });

    assertEquals("ok", events.get(0).getState());
  }
}
