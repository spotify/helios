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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

/**
 * This class is used by {@link HeliosSoloDeployment} to check whether the helios-solo master
 * is up.
 */
class SoloMasterProber {

  private static final Logger log = LoggerFactory.getLogger(SoloMasterProber.class);

  /**
   * @param hostAndPort {@link HostAndPort} object.
   * @return true if we can connect to the specified host and port. Null, otherwise.
   * @throws Exception
   */
  Boolean check(final HostAndPort hostAndPort) throws Exception {
    return check(hostAndPort, new Socket());
  }

  @VisibleForTesting
  Boolean check(final HostAndPort hostAndPort, final Socket socket) throws Exception {
    final SocketAddress address = new InetSocketAddress(hostAndPort.getHostText(),
                                                        hostAndPort.getPort());
    log.debug("attempting to connect to {}", address);

    try {
      socket.connect(address, 100);
      log.info("successfully connected to address {} for host and port {}", address, hostAndPort);
      return true;
    } catch (SocketTimeoutException | ConnectException e) {
      log.debug("could not connect to host and port {}: {}", hostAndPort, e.toString());
      return null;
    }
  }
}
