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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

public class AuthProviders {

  private static final Logger log = LoggerFactory.getLogger(AuthProviders.class);

  /**
   * Creates a service provider for server-side authentication.
   * Attempts to load it from a plugin if path is not null or using the app
   * class loader otherwise. If no authentication plugin was found,
   * returns a {@link NopServerAuthProvider}
   *
   * @param path Path to plugin jar.
   * @param serverName The name of the server using this {@link ServerAuthProvider}
   * @param secret String representing the secret for the auth
   * @return The {@link ServerAuthProvider} object.
   */
  public static ServerAuthProvider
  createServerAuthProvider(final Path path, final String serverName, final String secret) {
    return createAuthProviderFactory(path).createServerAuthProvider(serverName, secret);
  }

  /**
   * Creates a service provider for client-side authentication.
   * Attempts to load it from a plugin if path is not null or using the app
   * class loader otherwise. If no authentication plugin was found,
   * returns a {@link NopClientAuthProvider}
   *
   * @param path Path to plugin jar.
   * @param keyPath Path to key file.
   * @param authServerUris URIs of the hosts doing the actual authentication.
   * @return The {@link ClientAuthProvider} object.
   */
  public static ClientAuthProvider
  createClientAuthProvider(final Path path,
                           final Path keyPath,
                           final List<URI> authServerUris) {
    return createAuthProviderFactory(path).createClientAuthProvider(keyPath, authServerUris);
  }

  private static AuthProviderFactory createAuthProviderFactory(final Path path) {
    return path == null ? createFactory() : createFactory(path);
  }

  /**
   * Get an authentication factory from a plugin.
   *
   * @param path The path to the plugin jar.
   * @return The {@link AuthProviderFactory} object.
   */
  private static AuthProviderFactory createFactory(final Path path) {
    final AuthProviderFactory factory;
    final Path absolutePath = path.toAbsolutePath();
    try {
      factory = AuthProviderLoader.load(absolutePath);
      final String name = factory.getClass().getName();
      log.info("Loaded authentication plugin: {} ({})", name, absolutePath);
    } catch (AuthProviderLoadingException e) {
      throw new RuntimeException("Unable to load authentication plugin: " + absolutePath, e);
    }
    return factory;
  }

  /**
   * Get an authentication factory from the application class loader.
   *
   * @return The {@link AuthProviderFactory} object.
   */
  private static AuthProviderFactory createFactory() {
    final AuthProviderFactory factory;
    final AuthProviderFactory installed;
    try {
      installed = AuthProviderLoader.load();
    } catch (AuthProviderLoadingException e) {
      throw new RuntimeException("Unable to load authentication", e);
    }
    if (installed == null) {
      log.info("No authentication plugin configured");
      factory = new NopAuthProviderFactory();
    } else {
      factory = installed;
      final String name = factory.getClass().getName();
      log.info("Loaded installed authentication: {}", name);
    }
    return factory;
  }
}
