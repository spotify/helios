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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperMetricsImplTest {

  @Mock MetricRegistry registry;
  @Mock Timer timer;

  @Test
  public void testTimer() throws Exception {
    when(registry.timer("group.zookeeper.timer")).thenReturn(timer);
    final ZooKeeperMetrics zkMetrics = new ZooKeeperMetricsImpl("group", registry);
    zkMetrics.updateTimer("timer", 100, TimeUnit.NANOSECONDS);
    verify(registry).timer(eq("group.zookeeper.timer"));
    verify(timer).update(100, TimeUnit.NANOSECONDS);
  }
}
