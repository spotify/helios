/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.auth.crt;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.keyprovider.KeyProvider;
import com.spotify.helios.auth.AuthenticationPlugin;

import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;

import java.util.Map;

@AutoService(AuthenticationPlugin.class)
public class CrtAuthenticationPlugin implements AuthenticationPlugin<CrtAccessToken> {

  private final Map<String, String> environment;

  public CrtAuthenticationPlugin() {
    this(System.getenv());
  }

  @VisibleForTesting
  protected CrtAuthenticationPlugin(Map<String, String> environment) {
    this.environment = environment;
  }

  @Override
  public String schemeName() {
    return "crtauth";
  }

  @Override
  public ServerAuthentication<CrtAccessToken> serverAuthentication() {
    // only validate the presence of environment variables when this method is called, as opposed to
    // in the constructor, as the client-side code will not use the same environment variables
    final String ldapUrl =  getRequiredEnv("CRTAUTH_LDAP_URL");
    final String ldapSearchPath = getRequiredEnv("CRTAUTH_LDAP_SEARCH_PATH");
    final String serverName = getRequiredEnv("CRTAUTH_SERVERNAME");
    final String secret = getRequiredEnv("CRTAUTH_SECRET");
    final String ldapFieldNameOfKey = getOptionalEnv("CRTAUTH_LDAP_KEY_FIELDNAME", "sshPublicKey");
    final int tokenLifetimeSecs = getOptionalEnv("CRTAUTH_TOKEN_LIFETIME_SECS", 540);

    final LdapContextSource contextSource = new LdapContextSource();
    contextSource.setUrl(ldapUrl);
    contextSource.setAnonymousReadOnly(true);
    contextSource.setCacheEnvironmentProperties(false);

    final LdapTemplate ldapTemplate = new LdapTemplate(contextSource);

    // TODO (mbrown): this should be general, support reading keys from flat files etc
    final KeyProvider keyProvider =
        new LdapKeyProvider(ldapTemplate, ldapSearchPath, ldapFieldNameOfKey);

    CrtAuthServer authServer = new CrtAuthServer.Builder()
        .setServerName(serverName)
        .setKeyProvider(keyProvider)
        .setSecret(secret.getBytes())
        .setTokenLifetimeInS(tokenLifetimeSecs)
        .build();

    return new CrtServerAuthentication(new CrtTokenAuthenticator(authServer), authServer);
  }

  private String getEnv(String name, boolean required) {
    if (required && !environment.containsKey(name)) {
      throw new IllegalArgumentException("Environment variable " + name + " is required");
    }
    return environment.get(name);
  }

  private String getRequiredEnv(String name) {
    return getEnv(name, true);
  }

  private String getOptionalEnv(String name, String defaultValue) {
    final String defined = getEnv(name, false);
    return defined != null ? defined : defaultValue;
  }

  private int getOptionalEnv(String name, int defaultValue) {
    final String defined = getEnv(name, false);
    if (defined == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(defined);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Value for " + name + " is not numeric");
    }
  }

  @Override
  public ClientAuthentication clientAuthentication() {
    return null;
  }
}
