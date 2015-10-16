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

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.helios.auth.AuthenticationPlugin.ServerAuthentication;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.spi.inject.InjectableProvider;

import io.dropwizard.auth.Auth;
import io.dropwizard.jersey.setup.JerseyEnvironment;

public class CrtServerAuthentication implements ServerAuthentication<CrtAccessToken> {

  private final CrtTokenAuthenticator authenticator;
  private final CrtAuthServer authServer;

  public CrtServerAuthentication(CrtTokenAuthenticator authenticator, CrtAuthServer authServer) {
    this.authenticator = authenticator;
    this.authServer = authServer;
  }

  @Override
  public InjectableProvider<Auth, Parameter> authProvider() {
    // register the jersey injectable that run whenever a resource with @Auth is requested
    return new CrtAuthProvider(authenticator);
  }

  @Override
  public void registerAdditionalJerseyComponents(JerseyEnvironment env) {
    env.register(new CrtHandshakeResource(this.authServer));
  }

}
