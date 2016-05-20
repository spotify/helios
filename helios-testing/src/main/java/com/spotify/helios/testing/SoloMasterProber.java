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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;

/**
 * This class is used by {@link HeliosSoloDeployment} to check whether the helios-solo master
 * is up.
 */
class SoloMasterProber {

  private static final Logger log = LoggerFactory.getLogger(SoloMasterProber.class);

  /**
   * @param uri {@link URI} object.
   * @return true if we can connect to the specified uri. Null, otherwise.
   * @throws Exception
   */
  Boolean check(final URI uri) throws Exception {
    return check(uri, new Socket());
  }

  @VisibleForTesting
  Boolean check(final URI uri, final Socket socket) throws Exception {
    final SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
    log.debug("attempting to connect to {}", address);

    try {
      socket.connect(address, 100);
      log.info("successfully connected to address {} for uri {}", address, uri);
      return true;
    } catch (SocketTimeoutException | ConnectException e) {
      log.debug("could not connect to uri {}: {}", uri, e.toString());
      return null;
    }
  }
}
