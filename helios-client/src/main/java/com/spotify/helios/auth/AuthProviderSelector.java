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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Allows a client to avoid specifying which auth-scheme it wants to use upfront, deferring the
 * decision until we know which auth-scheme the server supports. Instead the user specifies all the
 * auth-schemes supported when constructing the
 * {@link com.spotify.helios.auth.AuthProviderSelector}.
 */
public class AuthProviderSelector implements AuthProvider.Factory {

  private static final Logger log = LoggerFactory.getLogger(AuthProviderSelector.class);

  // Scheme -> provider factory
  private final Map<String, AuthProvider.Factory> providerFactories;

  public AuthProviderSelector(final Map<String, AuthProvider.Factory> providerFactories) {
    this.providerFactories = providerFactories;
  }

  @Override
  public AuthProvider create(final String wwwAuthHeader, final AuthProvider.Context context) {
    // TODO(staffan): Support multiple comma-separated challenges
    final String authScheme = wwwAuthHeader.split(" ", 2)[0];

    final AuthProvider.Factory factory = providerFactories.get(authScheme);
    if (factory == null) {
      log.warn("Unsupported auth-scheme %s", authScheme);
      throw new IllegalArgumentException("Unsupported authentication scheme: " + authScheme);
    }

    final AuthProvider authProvider = factory.create(wwwAuthHeader, context);
    if (authProvider == null) {
      log.warn("AuthProvider.Factory returned null for auth-scheme %s", authScheme);
    } else {
      log.info("AuthProvider for scheme {} created", authScheme);
    }

    return authProvider;
  }
}
