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

package com.spotify.helios.master.resources;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.authentication.AuthHeader;
import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.authentication.HttpAuthenticator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import static com.google.common.base.Strings.isNullOrEmpty;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/_auth")
public class AuthResource {

  private static final Logger log = LoggerFactory.getLogger(AuthResource.class);

  private HttpAuthenticator httpAuthenticator;

  public AuthResource(final HttpAuthenticator httpAuthenticator) {
    this.httpAuthenticator = httpAuthenticator;
  }

  /**
   * TBA
   * @return A Response containing an auth challenge or token
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response handleAuthentication(@Context HttpHeaders headers) {
    final String httpHeaderKey = httpAuthenticator.getHttpAuthHeaderKey();
    final String authHeaderVal = headers.getRequestHeaders().getFirst(httpHeaderKey);
    final AuthHeader authHeader;
    try {
      authHeader = httpAuthenticator.parseHttpAuthHeaderValue(authHeaderVal);
    } catch (HeliosAuthException e) {
      return Response.status(400).entity(httpAuthenticator.badAuthHeaderMsg())
          .type(TEXT_PLAIN).build();
    }

    if (authHeader.getAction() == null || isNullOrEmpty(authHeader.getValue())) {
      return Response.status(400).entity(httpAuthenticator.badAuthHeaderMsg())
          .type(TEXT_PLAIN).build();
    }

    switch (authHeader.getAction()) {
      case REQUEST:
        try {
          // TODO (dxia) This can throw org.springframework.ldap.CommunicationException
          final String challenge = httpAuthenticator.createChallenge(authHeader.getValue());
          return Response.ok().header(httpHeaderKey, challenge).build();
        } catch (HeliosAuthException e) {
          log.info("HeliosAuthException: {}", e);
          return Response.status(400).entity("You did something wrong.").build();
        }
      case RESPONSE:
        try {
          final String token = httpAuthenticator.createToken(authHeader.getValue());
          return Response.ok().header(httpHeaderKey, token).build();
        } catch (HeliosAuthException e) {
          log.info("BadAuthException: {}", e);
          return Response.status(Response.Status.FORBIDDEN).entity(e.getMessage()).build();
        }
      default:
        return Response.status(400).entity("Unknown action " + authHeader.getAction()).build();
    }
  }
}
