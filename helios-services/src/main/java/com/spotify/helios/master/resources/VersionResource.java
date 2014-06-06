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

import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCheckResponse;
import com.spotify.helios.common.VersionCompatibility;
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("version")
public class VersionResource {

  @GET
  @Produces(TEXT_PLAIN)
  @Timed
  @ExceptionMetered
  public String version() {
    // wrap in double quotes to make valid json
    return String.format("\"%s\"", Version.POM_VERSION);
  }

  @GET
  @Path("/check")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public VersionCheckResponse versionCheck(@QueryParam("client") final String client) {
    final PomVersion clientVersion = PomVersion.parse(client);
    final PomVersion serverVersion = PomVersion.parse(Version.POM_VERSION);

    final VersionCompatibility.Status status = VersionCompatibility.getStatus(serverVersion,
                                                                              clientVersion);
    return new VersionCheckResponse(status, serverVersion, Version.RECOMMENDED_VERSION);
  }
}
