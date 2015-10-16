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

import com.google.auto.service.AutoService;

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.keyprovider.KeyProvider;
import com.spotify.helios.auth.AuthenticationPlugin;

import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;

import java.util.concurrent.TimeUnit;

@AutoService(AuthenticationPlugin.class)
public class CrtAuthenticationPlugin implements AuthenticationPlugin<CrtAccessToken> {

  @Override
  public String schemeName() {
    return "crtauth";
  }

  @Override
  public ServerAuthentication<CrtAccessToken> serverAuthentication() {
    // TODO (mbrown): check for null
    final String ldapUrl = System.getenv("CRTAUTH_LDAP_URL");
    final String ldapSearchPath = System.getenv("CRTAUTH_LDAP_SEARCH_PATH");
    final String serverName = System.getenv("CRTAUTH_SERVERNAME");
    final String secret = System.getenv("CRTAUTH_SECRET");
    final String ldapFieldNameOfKey = System.getenv("CRTAUTH_LDAP_KEY_FIELDNAME");

    final LdapContextSource contextSource = new LdapContextSource();
    contextSource.setUrl(ldapUrl);
    contextSource.setAnonymousReadOnly(true);
    contextSource.setCacheEnvironmentProperties(false);

    final LdapTemplate ldapTemplate = new LdapTemplate(contextSource);

    // TODO (mbrown): this should be general, support reading from flat files etc
    final KeyProvider keyProvider =
        new LdapKeyProvider(ldapTemplate, ldapSearchPath, ldapFieldNameOfKey);

    CrtAuthServer authServer = new CrtAuthServer.Builder()
        .setServerName(serverName)
        .setKeyProvider(keyProvider)
        .setSecret(secret.getBytes())
        .setTokenLifetimeInS((int) TimeUnit.MINUTES.toSeconds(9))
        .build();

    return new CrtServerAuthentication(new CrtTokenAuthenticator(authServer), authServer);
  }

  @Override
  public ClientAuthentication<CrtAccessToken> clientAuthentication() {
    return null;
  }
}
