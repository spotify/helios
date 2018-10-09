/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import com.google.common.net.HostAndPort;
import com.spotify.helios.client.HeliosClient;

/**
 * A HeliosDeployment represents a collection of Helios masters and agents upon which jobs may be
 * run.
 */
public interface HeliosDeployment extends AutoCloseable {

  /**
   * A helios client connected to the master(s) of this helios deployment.
   *
   * @return {@link HeliosClient}
   */
  HeliosClient client();

  /**
   * Returns the host and port information that the deployment is available at.
   *
   * @return {@link HostAndPort}
   */
  // TODO (mbrown): should this be URI to capture scheme info also?
  HostAndPort address();

  /**
   * Undeploy (shut down) this Helios deployment.
   */
  void close();
}

