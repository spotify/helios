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

package com.spotify.helios.servicescommon.statistics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class ZooKeeperMetricsImpl implements ZooKeeperMetrics {

  private static final String TYPE = "zookeeper";

  private final String prefix;
  private final Counter transientErrorCounter;
  private final Meter transientErrorMeter;
  private final MetricRegistry registry;

  public ZooKeeperMetricsImpl(final String group, final MetricRegistry registry) {
    prefix = MetricRegistry.name(group, TYPE) + ".";
    this.registry = registry;
    transientErrorCounter = registry.counter(prefix + "transient_error_count");
    transientErrorMeter = registry.meter(prefix + "transient_error_meter");
  }

  @Override
  public void zookeeperTransientError() {
    transientErrorCounter.inc();
    transientErrorMeter.mark();
  }

  @Override
  public void updateTimer(final String name, final long duration, final TimeUnit timeUnit) {
    registry.timer(prefix + name).update(duration, timeUnit);
  }

  @Override
  public void updateMeter(final String name) {
    registry.meter(prefix + name).mark();
  }
}
