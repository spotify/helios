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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;

import com.spotify.helios.client.HttpsHandlers.CertificateFileHttpsHandler;
import com.spotify.helios.client.HttpsHandlers.SshAgentHttpsHandler;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import static com.google.common.io.Resources.getResource;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticatingHttpConnectorTest {

  private static final String USER = "user";
  private static final Path CERTIFICATE_PATH = Paths.get(getResource("UIDCACert.pem").getPath());
  private static final Path KEY_PATH = Paths.get(getResource("UIDCACert.key").getPath());

  private final DefaultHttpConnector connector = mock(DefaultHttpConnector.class);
  private final String method = "GET";
  private final byte[] entity = new byte[0];
  private final ImmutableMap<String, List<String>> headers = ImmutableMap.of();

  private List<Endpoint> endpoints;

  @Before
  public void setUp() throws Exception {
    endpoints = ImmutableList.of(
        endpoint(new URI("https://server1.example"), InetAddresses.forString("192.168.0.1")),
        endpoint(new URI("https://server2.example"), InetAddresses.forString("192.168.0.2"))
    );
  }

  private AuthenticatingHttpConnector createAuthenticatingConnector(
      final Optional<AgentProxy> proxy, final List<Identity> identities) {

    final EndpointIterator endpointIterator = EndpointIterator.of(endpoints);
    return new AuthenticatingHttpConnector(USER, proxy, Optional.<Path>absent(),
                                           Optional.<Path>absent(), endpointIterator, connector,
                                           identities);
  }

  private AuthenticatingHttpConnector createAuthenticatingConnectorWithCertFile() {

    final EndpointIterator endpointIterator = EndpointIterator.of(endpoints);

    return new AuthenticatingHttpConnector(USER, Optional.<AgentProxy>absent(),
                                           Optional.of(CERTIFICATE_PATH), Optional.of(KEY_PATH),
                                           endpointIterator, connector);
  }

  private CustomTypeSafeMatcher<URI> matchesAnyEndpoint(final String path) {
    return new CustomTypeSafeMatcher<URI>("A URI matching one of the endpoints in " + endpoints) {
      @Override
      protected boolean matchesSafely(final URI item) {
        for (Endpoint endpoint : endpoints) {
          final InetAddress ip = endpoint.getIp();
          final URI uri = endpoint.getUri();

          if (item.getScheme().equals(uri.getScheme()) &&
              item.getHost().equals(ip.getHostAddress()) &&
              item.getPath().equals(path)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private Identity mockIdentity() {
    final Identity identity = mock(Identity.class);
    when(identity.getComment()).thenReturn("a comment");
    return identity;
  }

  @Test
  public void testNoIdentities_ResponseIsOK() throws Exception {
    final AuthenticatingHttpConnector authConnector = createAuthenticatingConnector(
        Optional.<AgentProxy>absent(),
        ImmutableList.<Identity>of());

    final String path = "/foo/bar";

    final HttpsURLConnection connection = mock(HttpsURLConnection.class);
    when(connector.connect(argThat(matchesAnyEndpoint(path)),
                           eq(method),
                           eq(entity),
                           eq(headers))
    ).thenReturn(connection);
    when(connection.getResponseCode()).thenReturn(200);

    final URI uri = new URI("https://helios" + path);

    authConnector.connect(uri, method, entity, headers);

    verify(connector, never()).setExtraHttpsHandler(any(HttpsHandler.class));
  }

  @Test
  public void testCertFile_ResponseIsOK() throws Exception {
    final AuthenticatingHttpConnector authConnector = createAuthenticatingConnectorWithCertFile();

    final String path = "/foo/bar";

    final HttpsURLConnection connection = mock(HttpsURLConnection.class);
    when(connector.connect(argThat(matchesAnyEndpoint(path)),
                           eq(method),
                           eq(entity),
                           eq(headers))
    ).thenReturn(connection);
    when(connection.getResponseCode()).thenReturn(200);

    final URI uri = new URI("https://helios" + path);

    authConnector.connect(uri, method, entity, headers);

    verify(connector).setExtraHttpsHandler(certFileHttpsHandlerWithArgs(
        USER, CERTIFICATE_PATH, KEY_PATH));
  }

  @Test
  public void testOneIdentity_ResponseIsOK() throws Exception {

    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mockIdentity();

    final AuthenticatingHttpConnector authConnector =
        createAuthenticatingConnector(Optional.of(proxy), ImmutableList.of(identity));

    final String path = "/another/one";

    final HttpsURLConnection connection = mock(HttpsURLConnection.class);
    when(connector.connect(argThat(matchesAnyEndpoint(path)),
        eq(method),
        eq(entity),
        eq(headers))
    ).thenReturn(connection);
    when(connection.getResponseCode()).thenReturn(200);

    URI uri = new URI("https://helios" + path);

    authConnector.connect(uri, method, entity, headers);

    verify(connector).setExtraHttpsHandler(sshAgentHttpsHandlerWithArgs(USER, proxy, identity));
  }

  @Test
  public void testOneIdentity_ResponseIsUnauthorized() throws Exception {

    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mockIdentity();

    final AuthenticatingHttpConnector authConnector =
        createAuthenticatingConnector(Optional.of(proxy), ImmutableList.of(identity));

    final String path = "/another/one";

    final HttpsURLConnection connection = mock(HttpsURLConnection.class);
    when(connector.connect(argThat(matchesAnyEndpoint(path)),
        eq(method),
        eq(entity),
        eq(headers))
    ).thenReturn(connection);
    when(connection.getResponseCode()).thenReturn(401);

    URI uri = new URI("https://helios" + path);

    HttpURLConnection returnedConnection = authConnector.connect(uri, method, entity, headers);

    verify(connector).setExtraHttpsHandler(sshAgentHttpsHandlerWithArgs(USER, proxy, identity));

    assertSame("If there is only one identity do not expect any additional endpoints to "
               + "be called after the first returns Unauthorized",
        returnedConnection, connection);
  }

  private static Endpoint endpoint(final URI uri, final InetAddress ip) {
    return new Endpoint() {
      @Override
      public URI getUri() {
        return uri;
      }

      @Override
      public InetAddress getIp() {
        return ip;
      }
    };
  }

  private static HttpsHandler sshAgentHttpsHandlerWithArgs(
      final String user, final AgentProxy agentProxy, final Identity identity) {
    return argThat(new ArgumentMatcher<HttpsHandler>() {
      @Override
      public boolean matches(final Object handler) {
        if (!(handler instanceof SshAgentHttpsHandler)) {
          return false;
        }

        final SshAgentHttpsHandler authHandler = (SshAgentHttpsHandler) handler;
        return authHandler.getUser().equals(user) &&
               authHandler.getAgentProxy().equals(agentProxy) &&
               authHandler.getIdentity().equals(identity);
      }
    });
  }

  private static HttpsHandler certFileHttpsHandlerWithArgs(
      final String user, final Path certificatePath, final Path keyPath) {
    return argThat(new ArgumentMatcher<HttpsHandler>() {
      @Override
      public boolean matches(final Object handler) {
        if (!(handler instanceof HttpsHandlers.CertificateFileHttpsHandler)) {
          return false;
        }

        final CertificateFileHttpsHandler authHandler = (CertificateFileHttpsHandler) handler;
        return authHandler.getUser().equals(user) &&
               authHandler.getClientCertificatePath().equals(certificatePath) &&
               authHandler.getClientKeyPath().equals(keyPath);
      }
    });
  }

  @Test
  public void testOneIdentity_ServerReturns502BadGateway() throws Exception {
    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mockIdentity();

    final AuthenticatingHttpConnector authConnector =
        createAuthenticatingConnector(Optional.of(proxy), ImmutableList.of(identity));

    final String path = "/foobar";

    final HttpsURLConnection connection = mock(HttpsURLConnection.class);
    when(connector.connect(argThat(matchesAnyEndpoint(path)),
        eq(method),
        eq(entity),
        eq(headers))
    ).thenReturn(connection);
    when(connection.getResponseCode()).thenReturn(502);

    URI uri = new URI("https://helios" + path);

    HttpURLConnection returnedConnection = authConnector.connect(uri, method, entity, headers);

    assertSame("If there is only one identity do not expect any additional endpoints to "
               + "be called after the first returns Unauthorized",
        returnedConnection, connection);

  }
}
