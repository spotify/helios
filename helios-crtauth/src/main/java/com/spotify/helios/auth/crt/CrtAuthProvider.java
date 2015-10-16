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

import com.google.common.base.Optional;

import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.AuthenticationException;

/**
 * This class sets up Jersey to be able to call our Authenticator instance whenever a constructor or
 * method parameter is annotated with {@link Auth}.
 */
public class CrtAuthProvider implements InjectableProvider<Auth, Parameter> {

  private final CrtTokenAuthenticator tokenAuthenticator;

  public CrtAuthProvider(CrtTokenAuthenticator tokenAuthenticator) {
    this.tokenAuthenticator = tokenAuthenticator;
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable getInjectable(ComponentContext ic, Auth auth, Parameter parameter) {
    return new CrtAuthInjectable(tokenAuthenticator, auth.required());
  }

  private static class CrtAuthInjectable extends AbstractHttpContextInjectable<HeliosUser> {

    private static final Logger log = LoggerFactory.getLogger(CrtAuthInjectable.class);

    private final CrtTokenAuthenticator authenticator;
    private final boolean required;

    public CrtAuthInjectable(CrtTokenAuthenticator authenticator, boolean required) {
      this.authenticator = authenticator;
      this.required = required;
    }

    /**
     * The role of this method is to examine the HttpContext for the Authorization header,  turn
     * it into a CrtAccessToken instance if present, and run it through the Authenticator.
     */
    @Override
    public HeliosUser getValue(HttpContext c) {
      final String authHeader = c.getRequest().getHeaderValue(HttpHeaders.AUTHORIZATION);

      if (authHeader != null) {
        final String[] tokenParts = authHeader.split(":");
        if (tokenParts.length == 2) {
          final String prefix = tokenParts[0];

          if (prefix.equals("chap")) {
            final String token = tokenParts[1];
            try {
              final Optional<HeliosUser> result =
                  authenticator.authenticate(new CrtAccessToken(token));

              if (result.isPresent()) {
                return result.get();
              }
            } catch (AuthenticationException e) {
              log.warn("Error authenticating credentials", e);
              throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
            }
          }
        }
      }

      // if we have fallen through here, because the header is not present or is not in the
      // expected structure, return 401 if the auth token is required.
      if (required) {
        throw new WebApplicationException(
            Response.status(Status.UNAUTHORIZED)
                // TODO (mbrown): check if this header value is correct
                .header(HttpHeaders.WWW_AUTHENTICATE, "crtauth")
                .type(MediaType.TEXT_PLAIN_TYPE)
                .entity("Credentials are required to access this resource.")
                .build());
      }

      return null;
    }
  }

}
