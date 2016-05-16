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

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class that helps {@link HeliosSoloDeployment} maintain a connection to the watchdog process
 * in helios-solo container. Closing this connection causes helios-solo to clean itself up and kill
 * itself.
 */
class SoloWatchdogConnector {

  private static final Logger log = LoggerFactory.getLogger(SoloWatchdogConnector.class);

  static Socket connectWithTimeout(final String host, final int port,
                                   final long timeout, final TimeUnit timeUnit) {
    final AtomicReference<Socket> ar = new AtomicReference<>();
    try {
      Polling.awaitUnchecked(timeout, timeUnit, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          final Socket socket = new Socket();
          try {
            connect(socket, host, port);
            ar.set(socket);
            return true;
          } catch (IOException e) {
            return null;
          }
        }
      });
    } catch (TimeoutException e) {
      throw Throwables.propagate(e);
    }
    log.info("Connecting to helios-solo watchdog at {}:{}", host, port);
    return ar.get();
  }

  static void connect(final Socket s, final String host, final int port) throws IOException {
    // 1. Connect
    try {
      s.connect(new InetSocketAddress(host, port));

      // For whatever reason it seems like connections get "connected" even though there's
      // really nothing on the other end -- to detect this send and recv some data.
      final DataOutputStream writer = new DataOutputStream(s.getOutputStream());
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(s.getInputStream()));
      writer.writeBytes("HELO\n");
      writer.flush();
      final String line = reader.readLine();
      if (line.startsWith("HELO")) {
        log.info("Connected to helios-solo watchdog");
      } else {
        throw new IOException("We didn't get back the HELO we sent to the watchdog process.");
      }
    } catch (IOException e) {
      log.debug("Failed to connect to helios-solo watchdog");
      throw e;
    }

  }
}
