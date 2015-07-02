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

package com.spotify.helios.rollingupdate;

import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.servicescommon.Reactor.Callback;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Deploys and undeploys jobs to implement the desired deployment group state.
 */
public class RollingUpdateService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(RollingUpdateService.class);

  private static final long UPDATE_INTERVAL = SECONDS.toMillis(30);

  private final Reactor reactor;

  /**
   * Create a new RollingUpdateService.
   *
   * @param reactorFactory    The factory to use for creating reactors.
   */
  public RollingUpdateService(final ReactorFactory reactorFactory) {
    this.reactor = checkNotNull(reactorFactory.create("rollingupdate", new Update(),
                                                      UPDATE_INTERVAL),
                                "reactor");
  }

  @Override
  protected void startUp() throws Exception {
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  /**
   * Starts and stops supervisors to reflect the desired state. Called by the reactor.
   */
  private class Update implements Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      log.debug("running rolling update service, not doing anything");
    }
  }
}
