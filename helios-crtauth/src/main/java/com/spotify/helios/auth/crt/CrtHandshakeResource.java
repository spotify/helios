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

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.exceptions.ProtocolVersionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/_auth")
public class CrtHandshakeResource {

  private static final Logger log = LoggerFactory.getLogger(CrtHandshakeResource.class);

  private final CrtAuthServer authServer;

  public CrtHandshakeResource(CrtAuthServer authServer) {
    this.authServer = authServer;
  }

  @GET
  public Response handshake(@Context HttpHeaders headers) {
    // initial request should have X-CHAP: request:<blob>
    final String chap = headers.getRequestHeaders().getFirst("X-CHAP");
    if (chap != null) {
      if (chap.startsWith("request:")) {
        return sendChallenge(chap);
      } else if (chap.startsWith("response:")) {
        return sendResponse(chap);
      }
    }

    return Response.status(Status.BAD_REQUEST)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN)
        .entity("Bad handshake")
        .build();

  }

  private Response sendChallenge(String chapHeader) {
    final String requestPayload = splitHeader(chapHeader);
    try {
      final String challenge = authServer.createChallenge(requestPayload);
      return Response.ok()
          .header("X-CHAP", "challenge:" + challenge)
          .build();
    } catch (IllegalArgumentException | ProtocolVersionException e) {
      log.warn("unable to create challenge for request with Authorization header: {}",
          chapHeader,
          e);
      return Response.status(Status.BAD_REQUEST)
          .entity("Error in generating challenge for your request")
          .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN)
          .build();
    }
  }

  private Response sendResponse(String chapHeader) {
    final String responsePayload = splitHeader(chapHeader);
    try {
      final String token = authServer.createToken(responsePayload);
      return Response.ok()
          .header("X-CHAP", "token:" + token)
          .build();
    } catch (IllegalArgumentException | ProtocolVersionException e) {
      // NOTE: the crtauth library throws IllegalArgumentException for cases where the key is not
      // found, signing cannot be performed, etc
      log.warn("unable to create response for request with Authorization header: {}",
          chapHeader,
          e);
      return Response.status(Status.BAD_REQUEST)
          .entity("Error in generating challenge for your request")
          .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN)
          .build();
    }
  }

  private static String splitHeader(final String chapHeader) {
    return chapHeader.substring(chapHeader.indexOf(":") + 1);
  }
}
