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

package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.TimeUnit;

public class ZooKeeperMetricsImpl implements ZooKeeperMetrics {
  private static final String TYPE = "zookeeper";

  private final MetricName transientErrorCounterName;
  private final Counter transientErrorCounter;
  private final Meter transientErrorMeter;

  private final MetricName transientErrorMeterName;

  public ZooKeeperMetricsImpl(String group, MetricsRegistry registry) {
    transientErrorCounterName = new MetricName(group, TYPE, "transient_error_count");
    transientErrorMeterName = new MetricName(group, TYPE, "transient_error_meter");

    transientErrorCounter = registry.newCounter(transientErrorCounterName);
    transientErrorMeter = registry.newMeter(transientErrorMeterName, "transientErrors",
        TimeUnit.MINUTES);
  }

  @Override
  public void zookeeperTransientError() {
    transientErrorCounter.inc();
    transientErrorMeter.mark();
  }
}
