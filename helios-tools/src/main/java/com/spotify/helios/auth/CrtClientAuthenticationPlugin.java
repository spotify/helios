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

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;

import com.spotify.crtauth.CrtAuthClient;
import com.spotify.crtauth.agentsigner.AgentSigner;
import com.spotify.crtauth.exceptions.CrtAuthException;

@AutoService(ClientAuthenticationPlugin.class)
public class CrtClientAuthenticationPlugin implements ClientAuthenticationPlugin {

  @Override
  public String schemeName() {
    return "crtauth";
  }

  @Override
  public AuthProvider.Factory authProviderFactory() {
    final String authServer = System.getenv("AUTH_CRT_SERVER_NAME");
    return new AuthProvider.Factory() {
      @Override
      public AuthProvider create(final AuthProvider.Context context) {
        final CrtAuthClient crtClient;
        try {
          crtClient = new CrtAuthClient(new AgentSigner(), authServer);
        } catch (CrtAuthException e) {
          throw Throwables.propagate(e);
        }
        return new CrtAuthProvider(context.dispatcher(), crtClient, context.user());
      }
    };
  }
}
