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

package com.spotify.helios.master.metrics;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckGaugeTest {

  @Mock
  HealthCheckRegistry registry;

  @Test
  public void testHealthy() throws Exception {
    when(registry.runHealthCheck(anyString())).thenReturn(HealthCheck.Result.healthy());
    final HealthCheckGauge gauge = new HealthCheckGauge(registry, "foo");
    assertThat(gauge.getValue(), equalTo(1));
  }

  @Test
  public void testUnhealthy() throws Exception {
    when(registry.runHealthCheck(anyString())).thenReturn(HealthCheck.Result.unhealthy("meh"));
    final HealthCheckGauge gauge = new HealthCheckGauge(registry, "foo");
    assertThat(gauge.getValue(), equalTo(0));
  }
}
