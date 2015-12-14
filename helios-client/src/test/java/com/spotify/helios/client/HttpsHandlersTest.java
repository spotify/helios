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

package com.spotify.helios.client;

import com.spotify.helios.client.HttpsHandlers.SshAgentHttpsHandler;
import com.spotify.helios.client.tls.SshAgentSSLSocketFactory;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import sun.security.ssl.SSLSocketFactoryImpl;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import static com.google.common.io.Resources.getResource;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HttpsHandlersTest {

  @Test
  public void testCertificateFile() throws Exception {
    final HttpsURLConnection conn = mock(HttpsURLConnection.class);

    final Path certificate = Paths.get(getResource("UIDCACert.pem").getPath());
    final Path key = Paths.get(getResource("UIDCACert.key").getPath());
    final HttpsHandlers.CertificateFileHttpsHandler h =
        new HttpsHandlers.CertificateFileHttpsHandler("foo", certificate, key);

    h.handle(conn);
    verify(conn).setSSLSocketFactory(any(SSLSocketFactoryImpl.class));
  }

  @Test
  public void testSshAgent() throws Exception {
    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mock(Identity.class);
    final HttpsURLConnection conn = mock(HttpsURLConnection.class);
    final SshAgentHttpsHandler h = new SshAgentHttpsHandler("foo", proxy, identity);

    h.handle(conn);
    verify(conn).setSSLSocketFactory(sshAgentSSLSocketFactoryWithArgs(proxy, identity, "foo"));
  }

  private static SSLSocketFactory sshAgentSSLSocketFactoryWithArgs(
      final AgentProxy proxy, final Identity identity, final String user) {
    return argThat(new ArgumentMatcher<SSLSocketFactory>() {
      @Override
      public boolean matches(final Object obj) {
        if (!(obj instanceof SshAgentSSLSocketFactory)) {
          return false;
        }

        final SshAgentSSLSocketFactory factory = (SshAgentSSLSocketFactory) obj;
        return factory.getAgentProxy().equals(proxy) &&
               factory.getIdentity().equals(identity) &&
               factory.getUsername().equals(user);
      }
    });
  }
}
