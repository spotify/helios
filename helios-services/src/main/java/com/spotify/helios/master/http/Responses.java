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

package com.spotify.helios.master.http;

import javax.ws.rs.WebApplicationException;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;

/**
 * Simpler utility code to signal error conditions from the resource classes.
 */
public class Responses {

  public static WebApplicationException badRequest(final Object entity) {
    return new WebApplicationException(status(BAD_REQUEST).entity(entity).build());
  }

  public static WebApplicationException badRequest() {
    return new WebApplicationException(BAD_REQUEST);
  }

  public static WebApplicationException notFound(final Object entity) {
    return new WebApplicationException(status(NOT_FOUND).entity(entity).build());
  }

  public static WebApplicationException notFound() {
    return new WebApplicationException(NOT_FOUND);
  }

  public static WebApplicationException forbidden(final Object entity) {
    return new WebApplicationException(status(FORBIDDEN).entity(entity).build());
  }

  public static WebApplicationException forbidden() {
    return new WebApplicationException(FORBIDDEN);
  }
}
