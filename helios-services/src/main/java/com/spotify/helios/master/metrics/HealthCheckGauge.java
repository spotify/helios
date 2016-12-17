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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

/**
 * This class reports the specified health check as a boolean gauge metric.
 */
public class HealthCheckGauge implements Gauge<Integer> {

  private final HealthCheckRegistry registry;
  private final String name;

  public HealthCheckGauge(final HealthCheckRegistry registry, final String name) {
    this.registry = registry;
    this.name = name;
  }

  @Override
  public Integer getValue() {
    final HealthCheck.Result result = registry.runHealthCheck(name);
    return result.isHealthy() ? 1 : 0;
  }
}



