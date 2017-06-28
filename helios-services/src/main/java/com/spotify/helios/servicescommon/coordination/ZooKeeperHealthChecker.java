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

package com.spotify.helios.servicescommon.coordination;

import com.codahale.metrics.health.HealthCheck;
import org.apache.curator.CuratorZookeeperClient;

public class ZooKeeperHealthChecker extends HealthCheck {

  private final CuratorZookeeperClient client;

  public ZooKeeperHealthChecker(final ZooKeeperClient zooKeeperClient) {
    this.client = zooKeeperClient.getCuratorFramework().getZookeeperClient();
  }

  @Override
  protected Result check() throws Exception {
    if (client.isConnected()) {
      return Result.healthy();
    } else {
      return Result.unhealthy("CuratorZookeeperClient reports that it is not connected");
    }
  }
}
