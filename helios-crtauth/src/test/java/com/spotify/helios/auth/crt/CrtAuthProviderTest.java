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

package com.spotify.helios.auth.crt;

import com.google.common.base.Optional;

import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import io.dropwizard.auth.Auth;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtAuthProviderTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  // mocks used in invoking the methods
  private final Auth auth = mock(Auth.class);
  private final HttpRequestContext requestContext = mock(HttpRequestContext.class);

  private final CrtTokenAuthenticator authenticator = mock(CrtTokenAuthenticator.class);
  private final CrtAuthProvider provider = new CrtAuthProvider(authenticator);

  private Object invokeAuthentication() {
    final Injectable injectable = provider.getInjectable(null, auth, null);

    assumeThat("Test has no value if the Injectable does not extend AbstractHttpContextInjectable",
        injectable, instanceOf(AbstractHttpContextInjectable.class));

    final HttpContext httpContext = mock(HttpContext.class);
    when(httpContext.getRequest()).thenReturn(requestContext);

    return ((AbstractHttpContextInjectable) injectable).getValue(httpContext);
  }

  @Test
  public void canAuthenticate() throws Exception {
    when(auth.required()).thenReturn(false);

    final String token = "tokken";
    final HeliosUser user = new HeliosUser("homer");
    when(requestContext.getHeaderValue("Authorization")).thenReturn("chap:" + token);
    when(authenticator.authenticate(new CrtAccessToken(token)))
        .thenReturn(Optional.of(user));

    final Object result = invokeAuthentication();
    assertThat(result, instanceOf(HeliosUser.class));
    assertThat((HeliosUser) result, is(user));
  }

  @Test
  public void authenticationNotRequired_NoCredentials() {
    when(auth.required()).thenReturn(false);
    when(requestContext.getHeaderValue("Authorization")).thenReturn(null);

    final Object result = invokeAuthentication();
    assertThat(result, nullValue());
  }

  @Test
  public void authenticationNotRequired_BadCredentials() throws Exception {
    when(auth.required()).thenReturn(false);
    when(requestContext.getHeaderValue("Authorization")).thenReturn("chap:chap");

    when(authenticator.authenticate(new CrtAccessToken("chap")))
        .thenReturn(Optional.<HeliosUser>absent());

    final Object result = invokeAuthentication();
    assertThat(result, nullValue());
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
    when(auth.required()).thenReturn(true);
    when(requestContext.getHeaderValue("Authorization")).thenReturn(null);

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    invokeAuthentication();
  }


  @Test
  public void authenticationRequired_BadCredentials() throws Exception {
    when(auth.required()).thenReturn(true);

    when(requestContext.getHeaderValue("Authorization")).thenReturn("chap:foobar");
    when(authenticator.authenticate(new CrtAccessToken("foobar")))
        .thenReturn(Optional.<HeliosUser>absent());

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    invokeAuthentication();
  }
}