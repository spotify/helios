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

import com.google.common.net.HostAndPort;

import com.spotify.helios.client.HeliosClient;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A HeliosDeploymentResource makes the supplied {@link HeliosDeployment} available to a JUnit
 * test, and guarantees to tear it down afterward.
 */
public class HeliosDeploymentResource extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(HeliosDeploymentResource.class);

  private final HeliosDeployment deployment;

  /**
   * @param deployment The Helios deployment to expose to your JUnit tests.
   */
  HeliosDeploymentResource(final HeliosDeployment deployment) {
    this.deployment = deployment;
  }

  /** Ensure that the HeliosDeployment is up. */
  @Override
  protected void before() throws Throwable {
    super.before();

    Polling.awaitUnchecked(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final HostAndPort hap = deployment.address();
        final SocketAddress address = new InetSocketAddress(hap.getHostText(), hap.getPort());
        log.debug("attempting to connect to {}", address);

        try {
          final Socket s = new Socket();
          s.connect(address, 100);
          log.info("successfully connected to HeliosDeployment at {}", s.getRemoteSocketAddress());
          return true;
        } catch (SocketTimeoutException | ConnectException e ) {
          log.debug("could not yet connect to HeliosDeployment: {}", e.toString());
          return null;
        }
      }
    });
  }

  @Override
  protected void after() {
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
