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

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

public class MetricsImpl implements Metrics {

  private static final String GROUP = "helios";

  private final SupervisorMetrics supervisorMetrics;
  private final MasterMetrics masterMetrics;
  private final ZooKeeperMetrics zooKeeperMetrics;
  private final JmxReporter jmxReporter;

  public MetricsImpl(final MetricsRegistry registry) {
    this.masterMetrics = new MasterMetricsImpl(GROUP, registry);
    this.supervisorMetrics = new SupervisorMetricsImpl(GROUP, registry);
    this.zooKeeperMetrics = new ZooKeeperMetricsImpl(GROUP, registry);
    this.jmxReporter = new JmxReporter(registry);
  }

  @Override
  public void start() {
    jmxReporter.start();
  }

  @Override
  public void stop() {
    jmxReporter.shutdown();
  }

  @Override
  public MasterMetrics getMasterMetrics() {
    return masterMetrics;
  }

  @Override
  public SupervisorMetrics getSupervisorMetrics() {
    return supervisorMetrics;
  }

  @Override
  public ZooKeeperMetrics getZooKeeperMetrics() {
    return zooKeeperMetrics;
  }
}
