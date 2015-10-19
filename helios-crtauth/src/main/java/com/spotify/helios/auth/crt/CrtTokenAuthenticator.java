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
import com.spotify.helios.auth.Authenticator;
import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpRequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;

import io.dropwizard.auth.AuthenticationException;

class CrtTokenAuthenticator implements Authenticator<CrtAccessToken> {

  private static final Logger log = LoggerFactory.getLogger(CrtTokenAuthenticator.class);

  private final CrtAuthServer crtAuthServer;

  public CrtTokenAuthenticator(CrtAuthServer crtAuthServer) {
    this.crtAuthServer = crtAuthServer;
  }

  @Override
  public Optional<CrtAccessToken> extractCredentials(final HttpRequestContext request) {
    final String authHeader = request.getHeaderValue(HttpHeaders.AUTHORIZATION);

    if (authHeader != null) {
      final String[] tokenParts = authHeader.split(":");
      if (tokenParts.length == 2) {
        final String prefix = tokenParts[0];

        if (prefix.equals("chap")) {
          final String token = tokenParts[1];
          return Optional.of(new CrtAccessToken(token));
        }
      }
    }

    return Optional.absent();
  }

  @Override
  public Optional<HeliosUser> authenticate(CrtAccessToken credentials)
      throws AuthenticationException {

    final String token = credentials.getToken();
    final String encodedUsername;
    try {
      encodedUsername = crtAuthServer.validateToken(token);
    } catch (TokenExpiredException | ProtocolVersionException | IllegalArgumentException e) {
      log.warn("error validating CRT token", e);
      return Optional.absent();
    }

    return Optional.of(new HeliosUser(encodedUsername));
  }
}
