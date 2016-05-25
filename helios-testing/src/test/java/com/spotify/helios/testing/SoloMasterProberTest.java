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

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;

import static java.lang.String.format;
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

  private static final String HOST = "192.168.0.1";
  private static final int PORT = 123;
  private static final URI MASTER_URI = URI.create("http://" + HOST + ":" + PORT);

  @Test
  public void testSuccess() throws Exception {
    final Socket socket = mock(Socket.class);
    final Boolean check = new SoloMasterProber().check(MASTER_URI, socket);
    verify(socket).connect(argThat(matchesHostAndPort(HOST, PORT)), anyInt());
    assertTrue(check);
  }

  @Test
  public void testSocketTimeout() throws Exception {
    final Socket socket = mock(Socket.class);
    doThrow(new SocketTimeoutException()).when(socket).connect(
        any(InetSocketAddress.class), anyInt());
    final Boolean check = new SoloMasterProber().check(MASTER_URI, socket);
    verify(socket).connect(argThat(matchesHostAndPort(HOST, PORT)), anyInt());
    assertThat(check, equalTo(null));
  }

  @Test
  public void testConnectException() throws Exception {
    final Socket socket = mock(Socket.class);
    doThrow(new ConnectException()).when(socket).connect(
        any(InetSocketAddress.class), anyInt());
    final Boolean check = new SoloMasterProber().check(MASTER_URI, socket);
    verify(socket).connect(argThat(matchesHostAndPort(HOST, PORT)), anyInt());
    assertThat(check, equalTo(null));
  }

  private CustomTypeSafeMatcher<InetSocketAddress> matchesHostAndPort(final String host,
                                                                      final int port) {
    return new CustomTypeSafeMatcher<InetSocketAddress>(
        format("An InetSocketAddress with host %s and port %d", host, port)) {
      @Override
      protected boolean matchesSafely(final InetSocketAddress inetAddr) {
        return inetAddr.getHostString().equals(host) && inetAddr.getPort() == port;
      }
    };
  }
}
