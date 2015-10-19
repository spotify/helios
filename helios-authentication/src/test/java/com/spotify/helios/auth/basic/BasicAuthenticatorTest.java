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

package com.spotify.helios.auth.basic;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpRequestContext;

import org.junit.Test;

import io.dropwizard.auth.basic.BasicCredentials;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicAuthenticatorTest {

  private final HttpRequestContext request = mock(HttpRequestContext.class);

  private final BasicAuthenticator authenticator =
      new BasicAuthenticator(ImmutableMap.of("admin", "password"));

  @Test
  public void extractCredentials_CorrectlyFormed() {
    when(request.getHeaderValue("Authorization")).thenReturn("Basic " + encode("foo", "bar"));

    assertThat(authenticator.extractCredentials(request),
        is(Optional.of(new BasicCredentials("foo", "bar"))));
  }

  private static String encode(String user, String pass) {
    final String s = user + ":" + pass;
    return BaseEncoding.base64().encode(s.getBytes());
  }

  @Test
  public void extractCredentials_Misformed() {
    when(request.getHeaderValue("Authorization")).thenReturn("Basic foo");

    assertThat(authenticator.extractCredentials(request),
        is(Optional.<BasicCredentials>absent()));
  }

  @Test
  public void extractCredentials_NotPresent() {
    when(request.getHeaderValue("Authorization")).thenReturn(null);

    assertThat(authenticator.extractCredentials(request),
        is(Optional.<BasicCredentials>absent()));
  }

  @Test
  public void testAuthenticate() {
    assertThat(authenticator.authenticate(new BasicCredentials("admin", "password")),
        is(Optional.of(new HeliosUser("admin"))));
  }

  @Test
  public void badAuthenticate() {
    assertThat(authenticator.authenticate(new BasicCredentials("admin", "pass")),
        is(Optional.<HeliosUser>absent()));
  }
}