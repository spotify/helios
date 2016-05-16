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

/**
 * This interface is used by {@link HeliosSoloDeployment} to check whether there are agents
 * available in helios-solo. The main purpose of this interface is to make HeliosSoloDeployment
 * easy to test by allowing us to easily mock the helios client calls to the master in helios-solo.
 */
interface SoloHostProber {

  /**
   * @param client {@link HeliosClient}
   * @param hostAndPort {@link HostAndPort}
   * @return true if we can connect to the specified host and port. Null, otherwise.
   * @throws Exception
   */
  Boolean check(HeliosClient client, HostAndPort hostAndPort) throws Exception;

}
