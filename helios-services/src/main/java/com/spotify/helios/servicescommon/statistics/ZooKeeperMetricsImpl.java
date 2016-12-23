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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.state.ConnectionState;

public class ZooKeeperMetricsImpl implements ZooKeeperMetrics {

  private static final String TYPE = "zookeeper";

  private final String prefix;
  private final Meter transientErrorMeter;
  private final Meter connectionStateChanged;
  private final MetricRegistry registry;

  public ZooKeeperMetricsImpl(final String group, final MetricRegistry registry) {
    this.prefix = MetricRegistry.name(group, TYPE) + ".";
    this.registry = registry;
    this.transientErrorMeter = registry.meter(prefix + "transient_error_meter");
    this.connectionStateChanged = registry.meter(prefix + "connection_state_changed");

    // create all of the meter instances immediately so that we report 0 values after a restart
    for (final ConnectionState state : ConnectionState.values()) {
      connectionStateMeter(state);
    }
  }

  @Override
  public void zookeeperTransientError() {
    transientErrorMeter.mark();
  }

  @Override
  public void updateTimer(final String name, final long duration, final TimeUnit timeUnit) {
    registry.timer(prefix + name).update(duration, timeUnit);
  }

  @Override
  public void connectionStateChanged(final ConnectionState newState) {
    connectionStateChanged.mark();
    connectionStateMeter(newState).mark();
  }

  private Meter connectionStateMeter(final ConnectionState state) {
    return registry.meter(prefix + "connection_state_" + state.name());
  }
}
