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

package com.spotify.helios.auth;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class AuthenticationPluginLoaderTest {

  private final ServerAuthenticationConfig config = new ServerAuthenticationConfig();

  @Test
  public void canLoadPlugin() {
    System.out.println(TestPlugin.class.getCanonicalName());
    config.setEnabledScheme("test-plugin");
    final AuthenticationPlugin<?> plugin = AuthenticationPluginLoader.load(config);

    assertThat(plugin, instanceOf(TestPlugin.class));
  }

  @Test(expected = IllegalStateException.class)
  public void pluginNotFound() {
    config.setEnabledScheme("something-that-does-not-exist");
    AuthenticationPluginLoader.load(config);
  }

  /** Plugin implementation used by tests above */
  public static class TestPlugin implements AuthenticationPlugin<String> {

    @Override
    public String schemeName() {
      return "test-plugin";
    }

    @Override
    public ServerAuthentication<String> serverAuthentication() {
      return null;
    }

    @Override
    public ClientAuthentication<String> clientAuthentication() {
      return null;
    }
  }
}