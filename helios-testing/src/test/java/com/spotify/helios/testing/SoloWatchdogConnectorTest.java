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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SoloWatchdogConnectorTest {

  private static final String HOST = "host";
  private static final int PORT = 123;

  @Test
  public void testConnectSucceeded() throws Exception {
    final Socket socket = mock(Socket.class);
    final byte[] bytes = "HELO\n".getBytes();
    when(socket.getInputStream()).thenReturn(new ByteArrayInputStream(bytes));
    when(socket.getOutputStream()).thenReturn(new ByteArrayOutputStream());

    new SoloWatchdogConnector().connect(socket, HOST, PORT);
    verify(socket).connect(Utils.socketWithHostAndPort(HOST, PORT));
  }

  @Test(expected = IOException.class)
  public void testConnectSocketFailed() throws Exception {
    final Socket socket = mock(Socket.class);
    doThrow(new IOException()).when(socket).connect(any(InetSocketAddress.class));

    new SoloWatchdogConnector().connect(socket, HOST, PORT);
    verify(socket).connect(Utils.socketWithHostAndPort(HOST, PORT));
  }

  @Test(expected = IOException.class)
  public void testConnectEchoFailed() throws Exception {
    final Socket socket = mock(Socket.class);
    final byte[] bytes = "FOO\n".getBytes();
    when(socket.getInputStream()).thenReturn(new ByteArrayInputStream(bytes));
    when(socket.getOutputStream()).thenReturn(new ByteArrayOutputStream());

    new SoloWatchdogConnector().connect(socket, HOST, PORT);
    verify(socket).connect(Utils.socketWithHostAndPort(HOST, PORT));
  }
}
