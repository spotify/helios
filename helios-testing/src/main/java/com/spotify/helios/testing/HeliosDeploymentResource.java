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

import com.spotify.helios.client.HeliosClient;

import org.junit.rules.ExternalResource;

/**
 * A HeliosDeploymentResource makes the supplied {@link HeliosDeployment} available to a JUnit
 * test, and guarantees to tear it down afterward.
 */
public class HeliosDeploymentResource extends ExternalResource {
  HeliosDeployment deployment;

  /**
   * @param deployment The Helios deployment to expose to your JUnit tests.
   */
  HeliosDeploymentResource(final HeliosDeployment deployment) {
    this.deployment = deployment;
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
