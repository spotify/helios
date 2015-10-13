/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.authentication.crtauth;

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.exceptions.ProtocolVersionException;
import com.spotify.helios.authentication.AuthHeader;
import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.authentication.HttpAuthenticator;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.authentication.crtauth.CrtAuthConfig.HEADER;

public class CrtHttpAuthenticator implements HttpAuthenticator {

  private final CrtAuthServer crtAuthServer;

  public CrtHttpAuthenticator(final CrtAuthServer crtAuthServer) {
    this.crtAuthServer = crtAuthServer;
  }

  @Override
  public String getHttpAuthHeaderKey() {
    return HEADER;
  }

  @Override
  public AuthHeader parseHttpAuthHeaderValue(final String header) throws HeliosAuthException {
    if (isNullOrEmpty(header)) {
      throw new HeliosAuthException("Empty " + HEADER + " from client.");
    }
    final String[] split = header.split(":");
    return new CrtAuthHeader(split[0], split[1]);
  }

  @Override
  public String createChallenge(final String request) throws HeliosAuthException {
    try {
      return "challenge:" + crtAuthServer.createChallenge(request);
    } catch (ProtocolVersionException e) {
      throw new HeliosAuthException("Bad CRT auth request input.", e);
    }
  }

  @Override
  public String createToken(final String response) throws HeliosAuthException {
    try {
      return "token:" + crtAuthServer.createToken(response);
    } catch (ProtocolVersionException e) {
      throw new HeliosAuthException("Bad CRT auth response input.", e);
    }
  }

  @Override
  public String badAuthHeaderMsg() {
    return HEADER + " headers must be of the form <request|response>:<base64-encoded value>";
  }
}
