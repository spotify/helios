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

@AutoService(ClientAuthenticationPlugin.class)
public class BasicClientAuthenticationPlugin implements ClientAuthenticationPlugin {

  @Override
  public String schemeName() {
    return "Basic";
  }

  @Override
  public AuthProvider.Factory authProviderFactory() {
    // TODO(staffan): Should we just use the "normal" user (as passed to the CLI) here?
    final String username = System.getenv("AUTH_BASIC_USERNAME");
    final String password = System.getenv("AUTH_BASIC_PASSWORD");

    if (username == null || password == null) {
      return null;
    } else {
      return new AuthProvider.Factory() {
        @Override
        public AuthProvider create(final AuthProvider.Context context) {
          return new BasicAuthProvider(username, password);
        }
      };
    }
  }
}
