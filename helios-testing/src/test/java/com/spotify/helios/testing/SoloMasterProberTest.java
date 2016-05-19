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

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SoloMasterProberTest {

  private static final String HOST = "host";
  private static final int PORT = 123;

  @Test
  public void testSuccess() throws Exception {
    final Socket socket = mock(Socket.class);
    final HostAndPort hostAndPort = HostAndPort.fromParts(HOST, PORT);
    final Boolean check = new SoloMasterProber().check(hostAndPort, socket);
    verify(socket).connect(socketWithHostAndPort(HOST, PORT), anyInt());
    assertTrue(check);
  }

  @Test
  public void testSocketTimeout() throws Exception {
    final Socket socket = mock(Socket.class);
    doThrow(new SocketTimeoutException()).when(socket).connect(
        any(InetSocketAddress.class), anyInt());
    final HostAndPort hostAndPort = HostAndPort.fromParts(HOST, PORT);
    final Boolean check = new SoloMasterProber().check(hostAndPort, socket);
    verify(socket).connect(socketWithHostAndPort(HOST, PORT), anyInt());
    assertThat(check, equalTo(null));
  }

  @Test
  public void testConnectException() throws Exception {
    final Socket socket = mock(Socket.class);
    doThrow(new ConnectException()).when(socket).connect(
        any(InetSocketAddress.class), anyInt());
    final HostAndPort hostAndPort = HostAndPort.fromParts(HOST, PORT);
    final Boolean check = new SoloMasterProber().check(hostAndPort, socket);
    verify(socket).connect(socketWithHostAndPort(HOST, PORT), anyInt());
    assertThat(check, equalTo(null));
  }

  private static InetSocketAddress socketWithHostAndPort(final String host, final int port) {
    return argThat(new ArgumentMatcher<InetSocketAddress>() {
      @Override
      public boolean matches(Object argument) {
        if (argument instanceof InetSocketAddress) {
          final InetSocketAddress inetAddr = (InetSocketAddress) argument;
          if (inetAddr.getHostString().equals(host) && inetAddr.getPort() == port) {
            return true;
          }
        }
        return false;
      }
    });
  }
}
