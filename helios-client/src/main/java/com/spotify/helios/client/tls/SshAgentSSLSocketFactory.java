/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.client.tls;

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;

import javax.net.ssl.SSLSocketFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps our custom TLS functionality in a {@link javax.net.ssl.SSLSocketFactory}, specifically for
 * use with {@link javax.net.ssl.HttpsURLConnection#setSSLSocketFactory(SSLSocketFactory)}. Only
 * operations that are actually used by HttpsURLConnection are implemented.
 */
public class SshAgentSSLSocketFactory extends SSLSocketFactory {

  private final AgentProxy agentProxy;
  private final Identity identity;
  private final String username;

  /**
   * @param username The username to set in the UID field of generated X509 certificates.
   * @throws IOException Should never be thrown.
   */
  public SshAgentSSLSocketFactory(final AgentProxy agentProxy, final Identity identity,
                                  final String username)
      throws IOException {
    checkNotNull(agentProxy, "agentProxy");
    checkNotNull(identity, "identity");
    checkNotNull(username, "username");

    this.agentProxy = agentProxy;
    this.identity = identity;
    this.username = username;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException {
    final RecordingTlsClientProtocol protocol = new RecordingTlsClientProtocol(
        s.getInputStream(), s.getOutputStream(), new SecureRandom());
    final SshAgentTlsClient tlsClient = new SshAgentTlsClient(agentProxy, identity, username,
                                                              protocol);

    protocol.connect(tlsClient);
    return new SshAgentTlsSocket(tlsClient, s);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
      throws IOException, UnknownHostException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(InetAddress address, int port) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress,
                             int localPort)
      throws IOException {
    throw new UnsupportedOperationException();
  }
}
