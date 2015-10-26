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

package com.spotify.helios.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.spotify.crtauth.CrtAuthClient;
import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtAuthProviderTest {

  private static final String USERNAME = "foo";
  private static final String AUTH_URI = "https://helios/_auth";
  private static final String TOKEN = "TOKEN";
  private static final String CHALLENGE = "CHALLENGE";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final RequestDispatcher dispatcher = mock(RequestDispatcher.class);
  private CrtAuthProvider authProvider;
  private final CrtAuthClient authClient = mock(CrtAuthClient.class);

  @Before
  public void setup() throws Exception {
    authProvider = new CrtAuthProvider(dispatcher, authClient, USERNAME);
    when(authClient.createResponse(CHALLENGE)).thenReturn("RESPONSE");
  }

  @Test
  public void testSuccessfulAuth() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + CHALLENGE))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + TOKEN))));

    // Getting the current header without renewing first should return null
    assertEquals(authProvider.currentAuthorizationHeader(), null);

    // Just check the token starts with "chap:".
    // The token changes every time because it's time-based.
    final String authHeader = authProvider.renewAuthorizationHeader(null).get();
    assertThat(authHeader, containsString("chap:"));

    // Getting the current header after renewing it should return the same one.
    assertEquals(authHeader, authProvider.currentAuthorizationHeader());
  }

  @Test
  public void testBadChallengeStatus() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 404, null,
                     Collections.<String, List<String>>emptyMap())));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString("Got a 404 status code during the crtauth handshake."));
    authProvider.renewAuthorizationHeader(null).get();
  }

  @Test
  public void testMissingChallengeHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     Collections.<String, List<String>>emptyMap())));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + TOKEN))));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString(
        "Didn't get an HTTP \"X-CHAP\" header X-CHAP during the crtauth handshake."));
    authProvider.renewAuthorizationHeader(null).get();
  }

  @Test
  public void testMalformedChallengeHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("brogram:" + CHALLENGE))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token:" + TOKEN))));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString("Got an invalid HTTP X-CHAP header"));
    authProvider.renewAuthorizationHeader(null).get();
  }

  @Test
  public void testBadTokenStatus() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + CHALLENGE))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 404, null,
                     authHeaderOf("token:" + TOKEN))));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString("Got a 404 status code during the crtauth handshake."));
    authProvider.renewAuthorizationHeader(null).get();
  }

  @Test
  public void testMissingTokenHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + CHALLENGE))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     Collections.<String, List<String>>emptyMap())));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString(
        "Didn't get an HTTP \"X-CHAP\" header X-CHAP during the crtauth handshake."));
    authProvider.renewAuthorizationHeader(null).get();
  }

  @Test
  public void testMalformedTokenHeader() throws Exception {
    when(dispatcher.request(authRequestOfType("request:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null, authHeaderOf("challenge:" + CHALLENGE))));

    when(dispatcher.request(authRequestOfType("response:"))).thenReturn(immediateFuture(
        new Response("GET", new URI(AUTH_URI), 200, null,
                     authHeaderOf("token" + TOKEN))));

    exception.expect(ExecutionException.class);
    exception.expectMessage(containsString("Got an invalid HTTP X-CHAP header"));
    authProvider.renewAuthorizationHeader(null).get();
  }

  private static HeliosRequest authRequestOfType(final String name) {
    return argThat(new ArgumentMatcher<HeliosRequest>() {
      @Override
      public boolean matches(Object argument) {
        if (argument instanceof HeliosRequest) {
          final HeliosRequest req = (HeliosRequest) argument;
          final String authHeader = req.header("X-CHAP");
          if (!isNullOrEmpty(authHeader) && authHeader.startsWith(name)) {
            return true;
          }
        }
        return false;
      }
    });
  }

  private static Map<String, List<String>> authHeaderOf(final String value) {
    return ImmutableMap.<String, List<String>>of("X-CHAP", ImmutableList.of(value));
  }
}
