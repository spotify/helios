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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.state.ConnectionState;
import org.junit.Test;

public class ZooKeeperMetricsImplTest {

  private final MetricRegistry registry = new MetricRegistry();
  private final ZooKeeperMetrics metrics = new ZooKeeperMetricsImpl("group", registry);

  @Test
  public void testTimer() throws Exception {
    metrics.updateTimer("timer", 100, TimeUnit.NANOSECONDS);

    final String name = "group.zookeeper.timer";
    assertThat(registry.getTimers(), hasKey(name));

    final Timer timer = registry.timer(name);
    assertEquals(1, timer.getCount());
    assertArrayEquals(new long[]{ 100 }, timer.getSnapshot().getValues());
  }

  @Test
  public void testConnectionStateChanged() throws Exception {
    metrics.connectionStateChanged(ConnectionState.SUSPENDED);
    metrics.connectionStateChanged(ConnectionState.RECONNECTED);

    assertThat(registry.getMeters(), allOf(
        hasKey("group.zookeeper.connection_state_changed"),
        hasKey("group.zookeeper.connection_state_SUSPENDED"),
        hasKey("group.zookeeper.connection_state_RECONNECTED")
    ));

    assertEquals(2, registry.meter("group.zookeeper.connection_state_changed").getCount());
    assertEquals(1, registry.meter("group.zookeeper.connection_state_SUSPENDED").getCount());
    assertEquals(1, registry.meter("group.zookeeper.connection_state_RECONNECTED").getCount());
  }
}
