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

import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class CrtAuthenticationPluginTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final Map<String, String> requiredEnvArgs = ImmutableMap.of(
      "CRTAUTH_LDAP_URL", "ldap://server1",
      "CRTAUTH_LDAP_SEARCH_PATH", "foo",
      "CRTAUTH_SERVERNAME", "server1",
      "CRTAUTH_SECRET", "a secret password",
      "CRTAUTH_TOKEN_LIFETIME_SECS", "60"
  );

  @Test
  public void serverAuthentication_NoEnvironmentVariables() {
    exception.expect(IllegalArgumentException.class);

    final Map<String, String> env = ImmutableMap.of();

    final CrtAuthenticationPlugin plugin = new CrtAuthenticationPlugin(env);
    plugin.serverAuthentication();
  }

  @Test
  public void serverAuthentication_AllRequiredArgs() {
    final CrtAuthenticationPlugin plugin = new CrtAuthenticationPlugin(requiredEnvArgs);
    plugin.serverAuthentication();
  }

  @Test
  public void serverAuthentication_BadlyFormattedTokenLifetime() {
    exception.expect(IllegalArgumentException.class);

    final Map<String, String> env = new HashMap<>(requiredEnvArgs);
    env.put("CRTAUTH_TOKEN_LIFETIME_SECS", "asdf");

    final CrtAuthenticationPlugin plugin = new CrtAuthenticationPlugin(env);
    plugin.serverAuthentication();
  }
}