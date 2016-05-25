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

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A HeliosDeploymentResource makes the supplied {@link HeliosSoloDeployment} available to a JUnit
 * test, and guarantees to tear it down afterward.
 */
public class HeliosDeploymentResource extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(HeliosDeploymentResource.class);

  private final HeliosSoloDeployment deployment;

  /**
   * @param deployment The Helios deployment to expose to your JUnit tests.
   */
  public HeliosDeploymentResource(final HeliosSoloDeployment deployment) {
    this.deployment = deployment;
  }

  /** Ensure that the HeliosDeployment is up. */
  @Override
  public void before() throws Throwable {
    super.before();

    final HeliosClient client = client();

    // wait for the helios master to be available
    Polling.awaitUnchecked(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          client.listMasters().get();
          log.info("successfully connected to helios master");
          return true;
        } catch (final Exception e) {
          log.debug("could not yet connect to HeliosDeployment: {}", e.toString());
          return null;
        }
      }
    });

    // Ensure that at least one agent is available and UP in this HeliosDeployment.
    // This prevents continuing with the test when starting up helios-solo before the agent is
    // registered.
    Polling.awaitUnchecked(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
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
          log.debug("0 agents in {}, will retry", deployment);
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
    });
  }

  @Override
  public void after() {
    log.info("Tearing down {}", this.deployment);
    deployment.close();
  }

  /**
   *
   * @return A Helios client connected to the Helios deployment supplied when instantiating the
   * HeliosDeploymentResource.
   */
  public HeliosClient client() {
    return deployment.client();
  }

}
