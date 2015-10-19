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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;

import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import io.dropwizard.auth.Auth;

import static com.spotify.helios.auth.AuthInjectableProvider.HELIOS_VERSION_HEADER;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthInjectableProviderTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  // HttpRequestContext is extension of HttpHeaders interface, needs to be used to wire into
  // httpContext.getRequest()
  private final HttpRequestContext httpHeaders = mock(HttpRequestContext.class);

  @SuppressWarnings("unchecked")
  private final Authenticator<String> authenticator = mock(Authenticator.class);

  private final AuthInjectableProvider<String> providerAuthRequired =
      newProvider(Predicates.<HttpHeaders>alwaysTrue());

  private final AuthInjectableProvider<String> providerAuthNotRequired =
      newProvider(Predicates.<HttpHeaders>alwaysFalse());

  private AuthInjectableProvider<String> newProvider(Predicate<HttpHeaders> authRequired) {
    return new AuthInjectableProvider<>(authenticator, "foobar", authRequired);
  }

  private Object invokeAuthentication(AuthInjectableProvider<String> provider) {
    final Auth auth = mock(Auth.class);
    final Injectable injectable = provider.getInjectable(null, auth, null);

    assumeThat("Test has no value if the Injectable does not extend AbstractHttpContextInjectable",
        injectable, instanceOf(AbstractHttpContextInjectable.class));

    final HttpContext httpContext = mock(HttpContext.class);
    when(httpContext.getRequest()).thenReturn(httpHeaders);

    return ((AbstractHttpContextInjectable) injectable).getValue(httpContext);
  }

  @Test
  public void successfulAuthentication() throws Exception {
    final HeliosUser user = new HeliosUser("jamesbond");
    final String credentials = "secret-token";

    when(httpHeaders.getHeaderValue(HELIOS_VERSION_HEADER)).thenReturn("1.0.1");

    when(authenticator.extractCredentials(httpHeaders))
        .thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials))
        .thenReturn(Optional.of(user));

    final Object result = invokeAuthentication(providerAuthRequired);
    assertThat(result, instanceOf(HeliosUser.class));
    assertThat((HeliosUser) result, is(user));

    final Object result2 = invokeAuthentication(providerAuthNotRequired);
    assertThat(result2, instanceOf(HeliosUser.class));
    assertThat((HeliosUser) result2, is(user));
  }

  @Test
  public void authenticationNotRequired_NoCredentials() {
    when(httpHeaders.getHeaderValue(HELIOS_VERSION_HEADER)).thenReturn("0.9.0");

    when(authenticator.extractCredentials(httpHeaders)).thenReturn(Optional.<String>absent());

    final Object result = invokeAuthentication(providerAuthNotRequired);
    assertThat(result, nullValue());
  }

  @Test
  public void authenticationNotRequired_BadCredentials() throws Exception {
    when(httpHeaders.getHeaderValue(HELIOS_VERSION_HEADER)).thenReturn("0.9.0");

    final String credentials = "bad-password";

    when(authenticator.extractCredentials(httpHeaders))
        .thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials))
        .thenReturn(Optional.<HeliosUser>absent());

    final Object result = invokeAuthentication(providerAuthNotRequired);
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

    when(httpHeaders.getHeaderValue(HELIOS_VERSION_HEADER)).thenReturn("1.1.0");

    when(authenticator.extractCredentials(httpHeaders)).thenReturn(Optional.<String>absent());

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    invokeAuthentication(providerAuthRequired);
  }


  @Test
  public void authenticationRequired_BadCredentials() throws Exception {

    when(httpHeaders.getHeaderValue(HELIOS_VERSION_HEADER)).thenReturn("1.1.0");

    final String credentials = "foo";

    when(authenticator.extractCredentials(httpHeaders)).thenReturn(Optional.of(credentials));

    when(authenticator.authenticate(credentials)).thenReturn(Optional.<HeliosUser>absent());

    exception.expect(instanceOf(WebApplicationException.class));
    exception.expect(hasStatus(401));

    invokeAuthentication(providerAuthRequired);
  }
}