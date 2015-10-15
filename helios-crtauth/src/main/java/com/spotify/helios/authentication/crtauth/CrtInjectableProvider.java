/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.authentication.crtauth;

import com.google.common.base.Optional;

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

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;

import static com.spotify.helios.authentication.crtauth.CrtAuthConfig.HEADER;

/**
 * A Jersey provider for CRT authentication.
 *
 * @param <T> the principal type.
 */
public class CrtInjectableProvider<T> implements InjectableProvider<Auth, Parameter> {

  private static final Logger log = LoggerFactory.getLogger(CrtInjectableProvider.class);

  private static class CrtAuthInjectable<T> extends AbstractHttpContextInjectable<T> {

    private final Authenticator<String, T> authenticator;
    private final boolean required;

    private CrtAuthInjectable(final Authenticator<String, T> authenticator,
                              boolean required) {
      this.authenticator = authenticator;
      this.required = required;
    }

    @Override
    public T getValue(HttpContext c) {
      final String authHeader = c.getRequest().getHeaderValue(HttpHeaders.AUTHORIZATION);
      try {
        if (authHeader != null) {
          final Optional<T> result = authenticator.authenticate(authHeader);
          if (result.isPresent()) {
            return result.get();
          }
        }
      } catch (IllegalArgumentException e) {
        log.debug("Error decoding credentials", e);
      } catch (AuthenticationException e) {
        log.warn("Error authenticating credentials", e);
        throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
      }

      if (required) {
        throw new WebApplicationException(
            Response.status(Response.Status.UNAUTHORIZED)
                .entity("Credentials are required to access this resource.")
                .header(HttpHeaders.WWW_AUTHENTICATE, HEADER)
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build());
      }

      return null;
    }
  }

  private final Authenticator<String, T> authenticator;

  /**
   * Creates a new CrtAuthProvider with the given {@link Authenticator} and realm.
   *
   * @param authenticator the authenticator which will take the CRT auth token and
   *                      convert them into instances of {@code T}
   */
  public CrtInjectableProvider(final Authenticator<String, T> authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable<?> getInjectable(ComponentContext ic, Auth a, Parameter c) {
    return new CrtAuthInjectable<>(authenticator, a.required());
  }
}
