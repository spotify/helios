/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Optional;
import com.google.common.base.Predicates;

import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.spi.container.ContainerRequest;

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthenticationRequestFilterTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  // HttpRequestContext is extension of HttpHeaders interface, needs to be used to wire into
  // httpContext.getRequest()
  private final ContainerRequest request = mock(ContainerRequest.class);

  @SuppressWarnings("unchecked")
  private final Authenticator<String> authenticator = mock(Authenticator.class);

  private final AuthenticationRequestFilter filterAuthRequired =
      new AuthenticationRequestFilter(authenticator,
          "foobar",
          Predicates.<HttpRequestContext>alwaysTrue());

  private final AuthenticationRequestFilter filterAuthNotRequired =
      new AuthenticationRequestFilter(authenticator,
          "foobar",
          Predicates.<HttpRequestContext>alwaysFalse());


  @Test
  public void successfulAuthentication() throws Exception {
    final HeliosUser user = new HeliosUser("jamesbond");
    final String credentials = "secret-token";

    when(authenticator.extractCredentials(request))
        .thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials))
        .thenReturn(Optional.of(user));

    // when auth is required...
    assertThat(filterAuthRequired.filter(request), sameInstance(request));

    // ... and when disabled
    assertThat(filterAuthNotRequired.filter(request), sameInstance(request));
  }

  @Test
  public void authenticationNotRequired_NoCredentials() {
    when(authenticator.extractCredentials(request)).thenReturn(Optional.<String>absent());

    assertThat(filterAuthNotRequired.filter(request), sameInstance(request));
  }

  @Test
  public void authenticationNotRequired_BadCredentials() throws Exception {
    final String credentials = "bad-password";

    when(authenticator.extractCredentials(request))
        .thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials))
        .thenReturn(Optional.<HeliosUser>absent());

    assertThat(filterAuthNotRequired.filter(request), sameInstance(request));
  }

  private static class StatusCodeMatcher extends CustomTypeSafeMatcher<WebApplicationException> {

    private final int expectedStatus;

    private StatusCodeMatcher(int expectedStatus) {
      super("A WebApplicationException with statusCode=" + expectedStatus);
      this.expectedStatus = expectedStatus;
    }

    @Override
    protected boolean matchesSafely(WebApplicationException item) {
      final Response response = item.getResponse();
      return response.getStatus() == expectedStatus;
    }
  }

  private static StatusCodeMatcher hasStatus(int sc) {
    return new StatusCodeMatcher(sc);
  }


  @Test
  public void authenticationRequired_NoCredentials() throws Exception {
    when(authenticator.extractCredentials(request)).thenReturn(Optional.<String>absent());

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    filterAuthRequired.filter(request);
  }


  @Test
  public void authenticationRequired_BadCredentials() throws Exception {
    final String credentials = "foo";

    when(authenticator.extractCredentials(request)).thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials)).thenReturn(Optional.<HeliosUser>absent());

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    filterAuthRequired.filter(request);
  }

}