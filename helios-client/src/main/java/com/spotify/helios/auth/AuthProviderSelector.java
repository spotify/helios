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

import com.spotify.helios.client.RequestDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;

/**
 * Allows a client to avoid specifying which auth-scheme it wants to use upfront, deferring the
 * decision until we know which auth-scheme the server supports. Instead the user specifies all the
 * auth-schemes supported when constructing the
 * {@link com.spotify.helios.auth.AuthProviderSelector}.
 *
 * When using this {@link com.spotify.helios.auth.AuthProvider} the first request will always be
 * unauthenticated.
 */
public class AuthProviderSelector implements AuthProvider {

  private static final Logger log = LoggerFactory.getLogger(AuthProviderSelector.class);

  private final RequestDispatcher requestDispatcher;
  // Scheme -> provider factory
  private final Map<String, Factory> providerFactories;
  private AtomicReference<ActiveProvider> activeProviderRef = new AtomicReference<>(null);

  public AuthProviderSelector(final RequestDispatcher requestDispatcher,
                              final Map<String, Factory> providerFactories) {
    this.requestDispatcher = requestDispatcher;
    this.providerFactories = providerFactories;
  }

  @Override
  public String currentAuthorizationHeader() {
    final ActiveProvider activeProvider = activeProviderRef.get();
    return activeProvider != null ? activeProvider.provider.currentAuthorizationHeader() : null;
  }

  @Override
  public ListenableFuture<String> renewAuthorizationHeader(final String authHeader) {
    // TODO(staffan): Support multiple comma-separated challegnes
    final String authScheme = authHeader.split(" ", 2)[0];

    ActiveProvider oldProvider, newProvider;
    do {
      oldProvider = this.activeProviderRef.get();
      if (oldProvider != null && authScheme.equals(oldProvider.scheme)) {
        // The existing provider has the same scheme -- nothing more to do.
        newProvider = oldProvider;
        break;
      }

      final Factory factory = providerFactories.get(authScheme);
      if (factory == null) {
        log.warn("Unsupported auth-scheme %s", authScheme);
        return immediateFailedFuture(
            new IllegalArgumentException("Unsupported authentication scheme: " + authScheme));
      }

      final AuthProvider authProvider = factory.create(requestDispatcher);
      if (authProvider == null) {
        log.warn("AuthProvider.Factory returned null for auth-scheme %s", authScheme);
        return immediateFailedFuture(
            new NullPointerException("AuthProvider.Factory returned null"));
      }

      newProvider = new ActiveProvider(authProvider, authScheme);
    } while (!this.activeProviderRef.compareAndSet(oldProvider, newProvider));

    if (newProvider != oldProvider) {
      log.info("Switched AuthProvider, %s -> %s",
               oldProvider == null ? "None" : oldProvider.scheme,
               newProvider.scheme);
    }

    return newProvider.provider.renewAuthorizationHeader(authHeader);
  }

  private static class ActiveProvider {

    final AuthProvider provider;
    final String scheme;

    private ActiveProvider(final AuthProvider provider, final String scheme) {
      this.provider = provider;
      this.scheme = scheme;
    }
  }
}
