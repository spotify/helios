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

package com.spotify.helios.testing;

import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class is used by {@link HeliosSoloDeployment} to check whether there are agents
 * available in helios-solo.
 */
class SoloHostProber {

  private static final Logger log = LoggerFactory.getLogger(SoloHostProber.class);

  /**
   * @param client {@link HeliosClient}
   * @return true if we can connect to the specified host and port. Null, otherwise.
   * @throws Exception
   */
  Boolean check(final HeliosClient client) throws Exception {
    final ListenableFuture<List<String>> future = client.listHosts();

    final List<String> hosts;
    try {
      // use a short timeout to allow this request to be retried a few times by the
      // Polling.await loop
      hosts = future.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException | InterruptedException e) {
      log.debug("timed out waiting for listHosts request to finish, will retry");
      return null;
    }

    if (hosts.isEmpty()) {
      log.debug("0 agents in {}, will retry");
      return null;
    }

    // Check that at least one host is UP (is maintaining a reasonably reliable
    // connection to ZK) in addition to registering.
    final ListenableFuture<Map<String, HostStatus>> statusFuture = client.hostStatuses(hosts);
    final Map<String, HostStatus> hostStatuses;
    try {
      hostStatuses = statusFuture.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException | InterruptedException e) {
      log.debug("timed out waiting for hostStatuses to finish, will retry");
      return null;
    }

    for (final HostStatus hostStatus : hostStatuses.values()) {
      if (hostStatus != null && hostStatus.getStatus() == HostStatus.Status.UP) {
        log.info("Ensured that at least one agent is UP in this HeliosDeployment, "
                 + "continuing with test!");
        return true;
      }
    }

    return null;
  }
}
