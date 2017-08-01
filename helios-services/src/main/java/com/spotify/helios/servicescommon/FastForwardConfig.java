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

package com.spotify.helios.servicescommon;

import com.google.common.net.HostAndPort;
import java.util.Optional;

public class FastForwardConfig {

  private final Optional<HostAndPort> address;
  private final int intervalSeconds;
  private final String key;

  public FastForwardConfig(Optional<String> address, int intervalSeconds, String key) {
    this.address = address.map(HostAndPort::fromString);
    this.intervalSeconds = intervalSeconds;
    this.key = key;
  }

  /**
   * Connection address, empty means use default.
   *
   * @return An {@link Optional} of {@link HostAndPort}
   */
  public Optional<HostAndPort> getAddress() {
    return address;
  }

  public int getReportingIntervalSeconds() {
    return intervalSeconds;
  }

  public String getMetricKey() {
    return key;
  }
}
