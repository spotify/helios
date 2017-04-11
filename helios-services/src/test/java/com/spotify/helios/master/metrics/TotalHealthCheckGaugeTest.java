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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.junit.Test;

public class TotalHealthCheckGaugeTest {

  @Test
  public void testAllHealthy() {
    final HealthCheckRegistry registry = new HealthCheckRegistry();
    registry.register("pass1", stubHealthCheck(HealthCheck.Result.healthy()));
    registry.register("pass2", stubHealthCheck(HealthCheck.Result.healthy()));

    final TotalHealthCheckGauge gauge = new TotalHealthCheckGauge(registry);
    assertThat(gauge.getValue(), is(1));
  }

  @Test
  public void testOneFails() {
    final HealthCheckRegistry registry = new HealthCheckRegistry();
    registry.register("pass1", stubHealthCheck(HealthCheck.Result.healthy()));
    registry.register("fail1", stubHealthCheck(HealthCheck.Result.unhealthy("error")));

    final TotalHealthCheckGauge gauge = new TotalHealthCheckGauge(registry);
    assertThat(gauge.getValue(), is(0));
  }

  @Test
  public void testAllFail() {
    final HealthCheckRegistry registry = new HealthCheckRegistry();
    registry.register("fail1", stubHealthCheck(HealthCheck.Result.unhealthy("error")));
    registry.register("fail2", stubHealthCheck(HealthCheck.Result.unhealthy("error")));

    final TotalHealthCheckGauge gauge = new TotalHealthCheckGauge(registry);
    assertThat(gauge.getValue(), is(0));
  }

  private static HealthCheck stubHealthCheck(HealthCheck.Result result) {
    return new HealthCheck() {
      @Override
      protected Result check() throws Exception {
        return result;
      }
    };
  }
}
