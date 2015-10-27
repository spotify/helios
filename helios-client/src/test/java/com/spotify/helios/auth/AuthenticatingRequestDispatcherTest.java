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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthenticatingRequestDispatcherTest {

  private static final URI TEST_URI;

  static {
    try {
      TEST_URI = new URL("http", "host", "/foo/bar").toURI();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static AuthProvider.Factory authProvider(final String key) {
    return authProvider(key, true);
  }

  private static AuthProvider.Factory authProvider(final String key,
                                                   final boolean initCredentials) {
    return new AuthProvider.Factory() {
      @Override
      public AuthProvider create(final String wwwAuthHeader, final AuthProvider.Context context) {
        return new TestAuthProvider(key, initCredentials);
      }
    };
  }

  private final AuthProvider mockAuthProvider = mock(AuthProvider.class);
  private final AuthProvider.Factory mockProviderFactory = new AuthProvider.Factory() {
    @Override
    public AuthProvider create(final String wwwAuthHeader,
                               final AuthProvider.Context context) {
      return mockAuthProvider;
    }
  };


  private final TestDispatcher delegate = new TestDispatcher("secret-key");
  private final HeliosRequest request = HeliosRequest.builder().uri(TEST_URI).build();

  @Test
  public void testNoAuthRequired() throws Exception {
    final TestDispatcher delegate = new TestDispatcher(null);

    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, authProvider("secret-key"), "user");

    final Response response = dispatcher.request(request).get();

    assertEquals(200, response.status());
    assertEquals(1, delegate.num200);
    assertEquals(0, delegate.num401);
  }

  @Test
  public void testAuthRequired() throws Exception {
    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, authProvider("secret-key"), "user");

    final Response response = dispatcher.request(request).get();

    assertEquals(200, response.status());
    assertEquals(1, delegate.num200);
    assertEquals(1, delegate.num401);
  }

  @Test
  public void testAuthProviderFactoryException() throws Exception {
    final TestDispatcher delegate = new TestDispatcher("secret-key");

    final AuthProvider.Factory factory = new AuthProvider.Factory() {
      @Override
      public AuthProvider create(final String wwwAuthHeader, final AuthProvider.Context context) {
        throw new IllegalStateException();
      }
    };

    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, factory, "user");

    Throwable cause = null;
    try {
      dispatcher.request(HeliosRequest.builder().uri(TEST_URI).build()).get();
    } catch (ExecutionException e) {
      cause = e.getCause();
    }

    assertNotNull(cause);
    assertTrue(cause.getCause() instanceof IllegalStateException);
  }

  @Test
  public void testAuthRequiredBadCredentials() throws Exception {
    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, authProvider("bad-key"), "user");

    final Response response = dispatcher.request(request).get();

    assertEquals(401, response.status());
    assertEquals(0, delegate.num200);
    assertEquals(2, delegate.num401);
  }

  @Test
  public void testNoInitialCredentials() throws Exception {
    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, authProvider("secret-key", false), "user");

    final Response response = dispatcher.request(request).get();

    assertEquals(200, response.status());

    assertEquals(1, delegate.num200);
    assertEquals(1, delegate.num401);
  }

  @Test
  public void testRenewalFailure() throws Exception {
    when(mockAuthProvider.renewAuthorizationHeader()).thenReturn(
        Futures.<String>immediateFailedFuture(new Exception("error")));

    final RequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, mockProviderFactory, "user");

    final Response response =
        dispatcher.request(HeliosRequest.builder().uri(TEST_URI).build()).get();

    assertEquals(401, response.status());

    assertEquals(0, delegate.num200);
    assertEquals(1, delegate.num401);
  }

  @Test
  public void testEagerAuthEnabled_HasGoodCurrentCredentials() throws Exception {
    final AuthenticatingRequestDispatcher dispatcher = new AuthenticatingRequestDispatcher(delegate,
        mockProviderFactory,
        "user",
        true,
        delegate.authScheme);

    when(mockAuthProvider.currentAuthorizationHeader()).thenReturn(delegate.credentials);

    final Response response = dispatcher.request(request).get();

    verify(mockAuthProvider, never()).renewAuthorizationHeader();

    assertEquals(200, response.status());
    assertEquals(1, delegate.numRequests);
    assertEquals(1, delegate.num200);
    assertEquals(0, delegate.num401);
  }

  @Test
  public void testEagerAuthEnabled_HasBadCurrentCredentials() throws Exception {
    // acts as if credentials are bad
    final TestDispatcher delegate = new TestDispatcher("oops");

    final AuthenticatingRequestDispatcher dispatcher =
        new AuthenticatingRequestDispatcher(delegate, mockProviderFactory, "user", true, this.delegate.authScheme);

    when(mockAuthProvider.currentAuthorizationHeader()).thenReturn(this.delegate.credentials);
    when(mockAuthProvider.renewAuthorizationHeader()).thenReturn(immediateFuture("oops"));

    final Response response = dispatcher.request(request).get();
    assertEquals(200, response.status());
    assertEquals(2, delegate.numRequests);
    assertEquals(1, delegate.num200);
    assertEquals(1, delegate.num401);
  }

  @Test
  public void testEagerAuthEnabled_HasNoCurrentCredentials() throws Exception {
    final AuthenticatingRequestDispatcher dispatcher = new AuthenticatingRequestDispatcher(delegate,
        mockProviderFactory,
        "user",
        true,
        delegate.authScheme);

    when(mockAuthProvider.currentAuthorizationHeader()).thenReturn(null);
    when(mockAuthProvider.renewAuthorizationHeader()).thenReturn(immediateFuture(delegate.credentials));

    final Response response = dispatcher.request(request).get();
    assertEquals(200, response.status());
    assertEquals(1, delegate.numRequests);
    assertEquals(1, delegate.num200);
    assertEquals(0, delegate.num401);
  }

  private static class TestAuthProvider implements AuthProvider {

    private final String credentials;
    private String curCredentials;

    private TestAuthProvider(final String credentials, final boolean initCredentials) {
      this.credentials = credentials;
      this.curCredentials = initCredentials ? credentials : null;
    }

    @Override
    public String currentAuthorizationHeader() {
      return curCredentials;
    }

    @Override
    public ListenableFuture<String> renewAuthorizationHeader() {
      curCredentials = credentials;
      return immediateFuture(curCredentials);
    }
  }

  private static class TestDispatcher implements RequestDispatcher {

    private final String authScheme;
    private final String credentials;
    private final boolean credentialsRequired;
    int numRequests;
    int num401;
    int num200;

    private TestDispatcher(final String credentials) {
      this.authScheme = "test-scheme";
      this.credentials = credentials;
      this.credentialsRequired = credentials != null;
    }

    @Override
    public ListenableFuture<Response> request(final HeliosRequest request) {
      numRequests++;
      final Map<String, List<String>> headers = Maps.newHashMap();
      if (!credentialsRequired ||
          credentials.equals(request.header(HttpHeaders.AUTHORIZATION))) {
        ++num200;
        return immediateFuture(new Response(request.method(), request.uri(), 200, null, headers));
      } else {
        ++num401;
        headers.put(HttpHeaders.WWW_AUTHENTICATE, Lists.newArrayList(authScheme));
        return immediateFuture(new Response(request.method(), request.uri(), 401, null, headers));
      }
    }

    @Override
    public void close() {}
  }
}
