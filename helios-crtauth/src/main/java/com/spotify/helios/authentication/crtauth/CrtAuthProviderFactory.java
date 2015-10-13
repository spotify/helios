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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.keyprovider.KeyProvider;
import com.spotify.helios.authentication.AuthClient;
import com.spotify.helios.authentication.AuthProviderFactory;
import com.spotify.helios.authentication.ClientAuthProvider;
import com.spotify.helios.authentication.HttpAuthenticator;
import com.spotify.helios.authentication.ServerAuthProvider;
import com.spotify.helios.authentication.User;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.spi.inject.InjectableProvider;

import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import io.dropwizard.auth.Auth;

public class CrtAuthProviderFactory implements AuthProviderFactory {

  private static final String DEFAULT_CONFIG_FILE_PATH1 = "/etc/default/helios-crtauth";
  private static final String DEFAULT_CONFIG_FILE_PATH2 = "helios-crtauth";

  @Override
  public ServerAuthProvider createServerAuthProvider(final String serverName, final String secret) {
    final ObjectMapper mapper = new ObjectMapper();
    final CrtAuthConfig config;
    try {
      File f = new File(DEFAULT_CONFIG_FILE_PATH1);
      if (!f.isFile() || !f.canRead()) {
        f = new File(DEFAULT_CONFIG_FILE_PATH2);
      }
      config = mapper.readValue(f, CrtAuthConfig.class);
      final LdapContextSource contextSource = new LdapContextSource();
      contextSource.setUrl(config.getLdapUrl());
      contextSource.setAnonymousReadOnly(true);
      contextSource.setCacheEnvironmentProperties(false);
      final LdapTemplate ldapTemplate = new LdapTemplate(contextSource);
      final KeyProvider keyProvider = new LdapKeyProvider(ldapTemplate, config.getLdapSearchPath());

      final CrtAuthServer crtAuthServer = new CrtAuthServer.Builder()
          .setServerName(serverName)
          .setKeyProvider(keyProvider)
          .setSecret(secret.getBytes())
          .build();

      final CrtAuthenticator authenticator = new CrtAuthenticator(crtAuthServer);
      final CrtInjectableProvider<User> userCrtAuthProvider =
          new CrtInjectableProvider<>(authenticator);

      return new CrtServerAuthProvider(userCrtAuthProvider,
                                       new CrtHttpAuthenticator(crtAuthServer));
    } catch (IOException e) {
      throw new RuntimeException(String.format(
          "Could not find or read config file at %s or %s.",
          DEFAULT_CONFIG_FILE_PATH1, DEFAULT_CONFIG_FILE_PATH2));
    }
  }

  @Override
  public ClientAuthProvider createClientAuthProvider(final Path privateKeyPath,
                                                     final List<URI> authServerUris) {
    return new CrtClientAuthProvider(privateKeyPath, authServerUris);
  }

  class CrtServerAuthProvider implements ServerAuthProvider {
    private InjectableProvider<Auth, Parameter> injectableProvider;
    private HttpAuthenticator httpAuthenticator;

    CrtServerAuthProvider(final InjectableProvider<Auth, Parameter> injectableProvider,
                          final HttpAuthenticator httpAuthenticator) {
      this.injectableProvider = injectableProvider;
      this.httpAuthenticator = httpAuthenticator;
    }

    public InjectableProvider<Auth, Parameter> getInjectableProvider() {
      return injectableProvider;
    }

    @Override
    public HttpAuthenticator getHttpAuthenticator() {
      return httpAuthenticator;
    }
  }

  class CrtClientAuthProvider implements ClientAuthProvider {

    private final Path privateKeyPath;
    private final List<URI> authServerUris;

    public CrtClientAuthProvider(final Path privateKeyPath, final List<URI> authServerUris) {
      this.privateKeyPath = privateKeyPath;
      this.authServerUris = authServerUris;
    }

    @Override
    public AuthClient getClient() {
      return new CrtAuthClientImpl(privateKeyPath, authServerUris);
    }
  }

}
