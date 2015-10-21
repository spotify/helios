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

import com.google.auto.service.AutoService;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.auth.AuthenticationPlugin;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import io.dropwizard.auth.basic.BasicCredentials;

import static com.google.common.base.Preconditions.checkNotNull;

/** Proof of concept for the authentication plugin framework using HTTP Basic Auth. */
@AutoService(AuthenticationPlugin.class)
public class BasicAuthenticationPlugin implements AuthenticationPlugin<BasicCredentials> {

  @Override
  public String cliSchemeName() {
    return "http-basic";
  }

  public String schemeName() {
    return "Basic";
  }

  @Override
  public ServerAuthentication<BasicCredentials> serverAuthentication(
      Map<String, String> environment) {

    final String path = environment.get("AUTH_BASIC_USERDB");
    checkNotNull(path,
        "Environment variable AUTH_BASIC_USERDB not defined, required for "
        + BasicAuthenticationPlugin.class.getSimpleName());

    return new BasicServerAuthentication(readFileOfUsers(path));
  }

  private Map<String, String> readFileOfUsers(final String path) {
    final File file = new File(path);
    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<Map<String, String>> typeToken = new TypeReference<Map<String, String>>() {
    };

    try {
      return objectMapper.readValue(file, typeToken);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
