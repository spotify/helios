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

package com.spotify.helios.auth.basic;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.auth.AuthenticationPlugin.ServerAuthentication;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.jersey.setup.JerseyEnvironment;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A very simple implementation of ServerAuthentication to demonstrate how to implement an
 * authentication plugin. This version reads in a user "database" from a JSON file store at a path
 * configured by an environment variable, making it likely far too simple to be used for anything
 * but demonstrations.
 */
public class BasicServerAuthentication implements ServerAuthentication<BasicCredentials> {

  private final Map<String, String> users;

  public BasicServerAuthentication() {
    final String path = System.getenv("AUTH_BASIC_USERDB");
    checkNotNull(path,
        "Environment variable AUTH_BASIC_USERDB not defined, required for "
        + BasicAuthenticationPlugin.class.getSimpleName());

    File file = new File(path);
    final ObjectMapper objectMapper = new ObjectMapper();

    try {
      this.users = objectMapper.readValue(file, new TypeReference<Map<String, String>>() {
      });
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public BasicServerAuthentication(Map<String, String> users) {
    this.users = users;
  }

  @Override
  public com.spotify.helios.auth.Authenticator<BasicCredentials> authenticator() {
    return new BasicAuthenticator(users);
  }

  @Override
  public void registerAdditionalJerseyComponents(JerseyEnvironment env) {
    // nothing to add
  }
}
