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

package com.spotify.helios.auth;

import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.transport.RequestDispatcher;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

public class AuthProviderSelector implements AuthProvider {

  private final RequestDispatcher requestDispatcher;
  // Scheme -> provider factory
  private final Map<String, Factory> providerFactories;
  private AtomicReference<AuthProvider> activeProvider = new AtomicReference<>(null);

  public AuthProviderSelector(final RequestDispatcher requestDispatcher,
                              final Map<String, Factory> providerFactories) {
    this.requestDispatcher = requestDispatcher;
    this.providerFactories = providerFactories;
  }

  @Override
  public String currentAuthorization() {
    final AuthProvider authProvider = activeProvider.get();
    return authProvider != null ? authProvider.currentAuthorization() : null;
  }

  @Override
  public ListenableFuture<String> renewAuthorization(final String authHeader) {
    // TODO(staffan): Support the case when multiple comma-separated auth schemes are returned in
    // the WWW-Authenticate header.
    // TODO(staffan): Log stuff. Which provider was requested, and if it becomes active or not.
    final String authScheme = authHeader.split(" ", 2)[0];
    AuthProvider authProvider;
    do {
      authProvider = activeProvider.get();
      if (authProvider != null) {
        break;
      }

      authProvider = providerFactories.get(authScheme).create(requestDispatcher);
      if (authProvider == null) {
        return immediateFailedFuture(
            new IllegalArgumentException("Unsupported authentication scheme: " + authScheme));
      }
    } while (!activeProvider.compareAndSet(null, authProvider));

    return authProvider.renewAuthorization(authHeader);
  }
}
