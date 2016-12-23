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

import com.spotify.helios.common.descriptors.PortMapping;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultProber implements Prober {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProber.class);

  @Override
  public boolean probe(final String host, final PortMapping portMapping) {
    final String protocol = portMapping.getProtocol();

    if (PortMapping.TCP.equals(protocol)) {
      return probeTcpPort(host, portMapping);
    } else if (PortMapping.UDP.equals(protocol)) {
      return probeUdpPort(host, portMapping);
    }

    throw new IllegalArgumentException("Unsupported protocol " + portMapping.getProtocol());
  }

  private boolean probeUdpPort(String host, PortMapping portMapping) {

    final Integer port = portMapping.getExternalPort();

    try {
      // Let's send a PING
      // A UDP service should ignore any messages that do not conform to its protocol
      // If it does then you probably should implement your own Prober or
      // skip the probing by using a Dummy prober
      final byte[] pingData = "PING".getBytes("UTF-8");

      // Use ephemeral port number
      final DatagramSocket serverSocket = new DatagramSocket(0);
      final SocketAddress socketAddr = new InetSocketAddress(host, port);
      serverSocket.connect(socketAddr);

      final InetAddress address = InetAddress.getByName(host);
      final DatagramPacket sendPacket =
          new DatagramPacket(pingData, pingData.length, address, port);
      serverSocket.send(sendPacket);

      // Wait for a response: This will cause either a timeout (OK) or a port not reachable (NOT OK)
      final byte[] receiveData = new byte[8];
      final DatagramPacket receivePacket =
          new DatagramPacket(receiveData, receiveData.length);
      serverSocket.setSoTimeout(200);
      serverSocket.receive(receivePacket);

    } catch (SocketTimeoutException e) {
      // OK we got a timeout. That means that the UDP port is up and listening
      return true;
    } catch (PortUnreachableException e) {
      // The port is unreachable which of course means that
      // there is no-one listening to the UDP port
      return false;
    } catch (SocketException e) {
      LOG.warn(e.getMessage(), e);
      return false;
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      return false;
    }
    return false;
  }

  private boolean probeTcpPort(String host, PortMapping portMapping) {
    final Integer port = portMapping.getExternalPort();
    try (final Socket ignored = new Socket(host, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
