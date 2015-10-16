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

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.exceptions.ProtocolVersionException;
import com.spotify.crtauth.exceptions.TokenExpiredException;
import com.spotify.helios.auth.HeliosUser;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtTokenAuthenticatorTest {

  private final CrtAuthServer authServer = mock(CrtAuthServer.class);
  private final CrtTokenAuthenticator authenticator = new CrtTokenAuthenticator(authServer);

  @Test
  public void testAuthenticate_Valid() throws Exception {
    final CrtAccessToken token = new CrtAccessToken("my-token");
    when(authServer.validateToken(token.getToken())).thenReturn("the-user");

    final Optional<HeliosUser> user = authenticator.authenticate(token);

    assertThat(user, notNullValue());
    assertThat(user.isPresent(), is(true));
    assertThat(user.get(), is(new HeliosUser("the-user")));
  }

  @Test
  public void testAuthenticate_TokenExpired() throws Exception {
    final CrtAccessToken token = new CrtAccessToken("my-token");
    when(authServer.validateToken(token.getToken())).thenThrow(new TokenExpiredException());

    final Optional<HeliosUser> user = authenticator.authenticate(token);

    assertThat(user, notNullValue());
    assertThat(user.isPresent(), is(false));
  }

  @Test
  public void testAuthenticate_BadProtocol() throws Exception {
    final CrtAccessToken token = new CrtAccessToken("my-token");
    when(authServer.validateToken(token.getToken()))
        .thenThrow(new ProtocolVersionException("uh oh"));

    final Optional<HeliosUser> user = authenticator.authenticate(token);

    assertThat(user, notNullValue());
    assertThat(user.isPresent(), is(false));
  }

  /**
   * crtauth-java docs state that IllegalArgumentException is thrown if the token "appears to be
   * tampered with" i.e. cannot be decoded.
   */
  @Test
  public void testAuthenticate_TamperedToken() throws Exception {

    final CrtAccessToken token = new CrtAccessToken("my-token");
    when(authServer.validateToken(token.getToken())).thenThrow(new IllegalArgumentException());

    final Optional<HeliosUser> user = authenticator.authenticate(token);

    assertThat(user, notNullValue());
    assertThat(user.isPresent(), is(false));
  }
}