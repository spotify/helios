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

package com.spotify.helios.authentication;

class NopHttpAuthenticator implements HttpAuthenticator {

  @Override
  public String getSchemeName() {
    return "noop";
  }

  @Override
  public boolean verifyToken(String username, String accessToken) {
    return true;
  }

  @Override
  public String getHttpAuthHeaderKey() {
    return null;
  }

  @Override
  public String createChallenge(final String request) {
    return null;
  }

  @Override
  public String createToken(final String response) {
    return null;
  }

  @Override
  public AuthHeader parseHttpAuthHeaderValue(final String header) {
    return new NopAuthHeader();
  }

  @Override
  public String badAuthHeaderMsg() {
    return "Server is using a noop authenticator (NopHttpAuthenticator). "
           + "Make sure your client is also using the default noop authenticator or else "
           + "you'll get these errors.";
  }
}
