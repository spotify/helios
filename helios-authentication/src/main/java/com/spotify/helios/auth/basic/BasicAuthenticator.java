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

package com.spotify.helios.auth.basic;

import com.google.common.base.Optional;

import com.spotify.helios.auth.Authenticator;
import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpRequestContext;

import org.eclipse.jetty.util.B64Code;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import io.dropwizard.auth.basic.BasicCredentials;

public class BasicAuthenticator implements Authenticator<BasicCredentials> {

  private static final Logger log = LoggerFactory.getLogger(BasicAuthenticator.class);

  private final Map<String, String> users;

  public BasicAuthenticator(final Map<String, String> users) {
    this.users = users;
  }

  @Override
  public Optional<BasicCredentials> extractCredentials(final HttpRequestContext request) {
    final String header = request.getHeaderValue(HttpHeaders.AUTHORIZATION);
    try {
      if (header != null) {
        final int space = header.indexOf(' ');
        if (space > 0) {
          final String method = header.substring(0, space);
          if ("Basic".equalsIgnoreCase(method)) {
            final String decoded = B64Code.decode(header.substring(space + 1),
                StringUtil.__ISO_8859_1);
            final int i = decoded.indexOf(':');
            if (i > 0) {
              final String username = decoded.substring(0, i);
              final String password = decoded.substring(i + 1);
              final BasicCredentials credentials = new BasicCredentials(username, password);
              return Optional.of(credentials);
            }
          }
        }
      }
    } catch (IllegalArgumentException e) {
      log.info("Error decoding credentials", e);
    }

    return Optional.absent();
  }

  @Override
  public Optional<HeliosUser> authenticate(final BasicCredentials credentials) {
    final String username = credentials.getUsername();
    final String password = credentials.getPassword();
    if (users.containsKey(username) && users.get(username).equals(password)) {
      return Optional.of(new HeliosUser(username));
    }
    return Optional.absent();
  }
}
