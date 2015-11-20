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

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticatingHttpConnectorTest {

  private final HttpConnector connector = mock(HttpConnector.class);
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
    return new AuthenticatingHttpConnector("user",
        proxy,
        identities,
        endpointIterator,
        connector);
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

    verify(connection, never()).setSSLSocketFactory(any(SSLSocketFactory.class));
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

    // TODO (mbrown): assert that the sslsocketfactory has a reference to *this* identity
    verify(connection).setSSLSocketFactory(any(SSLSocketFactory.class));
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

    verify(connection).setSSLSocketFactory(any(SSLSocketFactory.class));

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
}
