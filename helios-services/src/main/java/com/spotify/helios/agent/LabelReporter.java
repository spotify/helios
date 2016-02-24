/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.agent;

import com.spotify.helios.common.Json;
import com.spotify.helios.servicescommon.coordination.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Puts the labels the agent has been assigned into ZK so they can be
 * visible to the master and via the API.
 */
public class LabelReporter extends InterruptingScheduledService {

  private static final int RETRY_INTERVAL_MILLIS = 1000;

  private final Map<String, String> labels;
  private final ZooKeeperNodeUpdater nodeUpdater;
  private final CountDownLatch zkRegistrationSignal;

  public LabelReporter(final String host, final Map<String, String> labels,
                       final NodeUpdaterFactory nodeUpdaterFactory,
                       final CountDownLatch zkRegistrationSignal) {
    this.labels = labels;
    this.nodeUpdater = nodeUpdaterFactory.create(Paths.statusHostLabels(host));
    this.zkRegistrationSignal = checkNotNull(zkRegistrationSignal, "zkRegistrationSignal");
  }


  @Override
  protected void runOneIteration() throws InterruptedException {
    // Wait for the agent to register itself with ZooKeeper to prevent this reporter from winning a
    // race and then having its data erased.
    zkRegistrationSignal.await();
    final boolean succesful = nodeUpdater.update(Json.asBytesUnchecked(labels));
    if (succesful) {
      stopAsync();
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, RETRY_INTERVAL_MILLIS, MILLISECONDS);
  }
}
