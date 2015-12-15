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

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.net.ssl.SSLSocketFactory;

import static com.google.common.io.Resources.getResource;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class HttpsHandlersTest {

  @Test
  public void testCertificateFile() throws Exception {
    final Path certificate = Paths.get(getResource("UIDCACert.pem").getPath());
    final Path key = Paths.get(getResource("UIDCACert.key").getPath());
    final HttpsHandlers.CertificateFileHttpsHandler h =
        new HttpsHandlers.CertificateFileHttpsHandler("foo", certificate, key);

    final SSLSocketFactory sslSocketFactory = h.socketFactory();
    assertThat(sslSocketFactory, notNullValue());
    // TODO (mbrown): how else to test the SSLSocketFactory?
  }

  @Test
  public void testSshAgent() throws Exception {
    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mock(Identity.class);
    final SshAgentHttpsHandler h = new SshAgentHttpsHandler("foo", proxy, identity);

    final SSLSocketFactory sslSocketFactory = h.socketFactory();
    assertThat(sslSocketFactory, instanceOf(SshAgentSSLSocketFactory.class));
    assertThat((SshAgentSSLSocketFactory) sslSocketFactory,
        is(sshAgentFactory(proxy, identity, "foo")));

  }

  private static Matcher<SshAgentSSLSocketFactory> sshAgentFactory(
      final AgentProxy proxy, final Identity identity, final String user) {

    final String description =
        "A SshAgentSSLSocketFactory with user=" + user + " and identity=" + identity;

    return new CustomTypeSafeMatcher<SshAgentSSLSocketFactory>(description) {
      @Override
      protected boolean matchesSafely(final SshAgentSSLSocketFactory factory) {
        return factory.getAgentProxy().equals(proxy) &&
               factory.getIdentity().equals(identity) &&
               factory.getUsername().equals(user);
      }
    };
  }
}
