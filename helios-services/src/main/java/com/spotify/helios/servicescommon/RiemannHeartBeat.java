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

import com.yammer.dropwizard.lifecycle.Managed;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Periodically sends heart beat messages to Riemann.
 */
public class RiemannHeartBeat implements Managed {
  private final ScheduledExecutorService scheduler;
  private final int interval;
  private final TimeUnit timeUnit;
  private final RiemannFacade facade;

  public RiemannHeartBeat(final TimeUnit timeUnit, final int interval,
                          final RiemannFacade riemannFacade) {
    this.scheduler = getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1),
                                                        0, SECONDS);
    this.timeUnit = timeUnit;
    this.interval = interval;
    this.facade = riemannFacade.stack("heartbeat");
  }

  @Override
  public void start() throws Exception {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        facade.event()
            .state("ok")
            .metric(1.0)
            .tags("heartbeat")
            .ttl(timeUnit.toSeconds(interval * 3))
            .send();

      }
    }, 0, interval, timeUnit);
  }

  @Override
  public void stop() throws Exception {
    scheduler.shutdownNow();
  }

}
