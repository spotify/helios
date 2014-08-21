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

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;

/**
 * A wrapper around the Riemann client, but can pre-supply client and host name and can be used
 * to build up nested service names.
 */
public class RiemannFacade {
  private static final Integer EVENT_TIME_TO_LIVE = 600;

  private final AbstractRiemannClient client;
  private final String hostName;
  private final String service;

  public RiemannFacade(AbstractRiemannClient client, String hostName, String service) {
    this.client = client;
    this.hostName = hostName;
    this.service = service;
  }

  public EventDSL event() {
    return client.event()
        .time(System.currentTimeMillis() / 1000.0)
        .ttl(EVENT_TIME_TO_LIVE)
        .host(hostName)
        .service(service);
  }

  public RiemannFacade stack(String subService) {
    return new RiemannFacade(client, hostName, service + "/" + subService);
  }
}
