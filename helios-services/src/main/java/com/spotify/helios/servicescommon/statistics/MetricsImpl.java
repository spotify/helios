/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon.statistics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

public class MetricsImpl implements Metrics {

  public enum Type {
    MASTER, AGENT
  }

  private static final String GROUP = "helios";

  private final SupervisorMetrics supervisorMetrics;
  private final MasterMetrics masterMetrics;
  private final ZooKeeperMetrics zooKeeperMetrics;
  private final JmxReporter jmxReporter;

  public MetricsImpl(final MetricRegistry registry, final Type type) {
    // MasterMetrics is only for masters, and SupervisorMetrics only for agents
    this.masterMetrics = type == Type.MASTER ? new MasterMetricsImpl(GROUP, registry)
                                             : new NoopMasterMetrics();

    this.supervisorMetrics = type == Type.AGENT ? new SupervisorMetricsImpl(GROUP, registry)
                                                : new NoopSupervisorMetrics();

    this.zooKeeperMetrics = new ZooKeeperMetricsImpl(GROUP, registry);
    this.jmxReporter = JmxReporter.forRegistry(registry).build();
  }

  @Override
  public void start() {
    jmxReporter.start();
  }

  @Override
  public void stop() {
    jmxReporter.close();
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
