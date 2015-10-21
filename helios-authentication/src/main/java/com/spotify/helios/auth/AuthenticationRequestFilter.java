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

package com.spotify.helios.auth;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.dropwizard.auth.AuthenticationException;

public class AuthenticationRequestFilter implements ContainerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(AuthenticationRequestFilter.class);

  private final Authenticator authenticator;
  private final String scheme;
  private final Predicate<HttpRequestContext> isAuthRequired;

  public AuthenticationRequestFilter(final Authenticator authenticator,
                                     final String scheme,
                                     final Predicate<HttpRequestContext> isAuthRequired) {
    this.authenticator = authenticator;
    this.scheme = scheme;
    this.isAuthRequired = isAuthRequired;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ContainerRequest filter(final ContainerRequest request) {
    final Optional<Object> credentials = authenticator.extractCredentials(request);
    if (credentials.isPresent()) {
      try {
        final Optional<HeliosUser> result = authenticator.authenticate(credentials.get());

        if (result.isPresent()) {
          // authenticated!
          return request;
        }
      } catch (AuthenticationException e) {
        log.warn("Error authenticating credentials", e);
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
      }
    }

    // Either the request did not provide credentials or they did not map to a valid user.
    // Return a 401 Unauthorized response if the authentication is required for this user
    if (isAuthRequired.apply(request)) {
      throw new WebApplicationException(
          Response.status(Status.UNAUTHORIZED)
              .header(HttpHeaders.WWW_AUTHENTICATE, scheme)
              .type(MediaType.TEXT_PLAIN_TYPE)
              .entity("Credentials are required to access this resource.")
              .build());

    }
    // header was not supplied, or was invalid, but that's ok for this client version
    return request;
  }
}
