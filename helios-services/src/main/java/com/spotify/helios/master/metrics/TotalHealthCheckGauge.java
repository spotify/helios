/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.helios.master.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import java.util.SortedMap;

/**
 * Run all healthchecks, and report a healthy gauge value if and only if every healthcheck passes.
 */
public class TotalHealthCheckGauge implements Gauge<Integer> {

  private final HealthCheckRegistry registry;

  public TotalHealthCheckGauge(final HealthCheckRegistry registry) {
    this.registry = registry;
  }

  @Override
  public Integer getValue() {
    final SortedMap<String, HealthCheck.Result> results = registry.runHealthChecks();
    return results.values().stream().allMatch(HealthCheck.Result::isHealthy) ? 1 : 0;
  }
}
