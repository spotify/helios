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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

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

public class AuthInjectableProvider<C> implements InjectableProvider<Auth, Parameter> {

  public static final String HELIOS_VERSION_HEADER = "Helios-Version";

  private static final Logger log = LoggerFactory.getLogger(AuthInjectableProvider.class);

  private final Authenticator<C> authenticator;
  private final String scheme;
  private final Predicate<HttpHeaders> isAuthRequired;

  public AuthInjectableProvider(final Authenticator<C> authenticator,
                                final String scheme,
                                final Predicate<HttpHeaders> isAuthRequired) {
    this.authenticator = authenticator;
    this.scheme = scheme;
    this.isAuthRequired = isAuthRequired;
  }

  @Override
  public final ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable getInjectable(final ComponentContext ic, final Auth auth,
                                  final Parameter parameter) {
    return new AbstractHttpContextInjectable() {
      @Override
      public Object getValue(final HttpContext c) {
        HttpHeaders headers = c.getRequest();

        final Optional<C> credentials = authenticator.extractCredentials(headers);

        if (credentials.isPresent()) {
          try {
            final Optional<HeliosUser> result = authenticator.authenticate(credentials.get());

            if (result.isPresent()) {
              return result.get();
            }
          } catch (AuthenticationException e) {
            log.warn("Error authenticating credentials", e);
            throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
          }
        }

        // Either the request did not provide credentials or they did not map to a valid user.
        // Return a 401 Unauthorized response if the authentication is required for this user
        if (isAuthRequired.apply(headers)) {
          throw new WebApplicationException(
              Response.status(Status.UNAUTHORIZED)
                  .header(HttpHeaders.WWW_AUTHENTICATE, scheme)
                  .type(MediaType.TEXT_PLAIN_TYPE)
                  .entity("Credentials are required to access this resource.")
                  .build());

        }
        // header was not supplied, or was invalid, but that's ok for this client version
        return null;
      }
    };
  }
}
