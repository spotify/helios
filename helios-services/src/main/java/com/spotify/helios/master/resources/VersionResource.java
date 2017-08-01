/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.master.resources;

import static com.google.common.base.Strings.isNullOrEmpty;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCheckResponse;
import com.spotify.helios.common.VersionCompatibility;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("version")
public class VersionResource {

  /**
   * Returns the server version string.
   *
   * @return The server version.
   */
  @GET
  @Produces(TEXT_PLAIN)
  @Timed
  @ExceptionMetered
  public String version() {
    // wrap in double quotes to make valid json
    return String.format("\"%s\"", Version.POM_VERSION);
  }

  /**
   * Given the client version, returns the version status, i.e. whether or not they should be
   * compatible or not.
   *
   * @param client The client version.
   *
   * @return The VersionCheckResponse object.
   */
  @GET
  @Path("/check")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public VersionCheckResponse versionCheck(
      @QueryParam("client") @DefaultValue("") final String client) {
    final PomVersion serverVersion = PomVersion.parse(Version.POM_VERSION);
    final VersionCompatibility.Status status;

    if (isNullOrEmpty(client)) {
      return new VersionCheckResponse(VersionCompatibility.Status.MISSING,
          serverVersion, Version.RECOMMENDED_VERSION);
    }

    final PomVersion clientVersion = PomVersion.parse(client);
    status = VersionCompatibility.getStatus(serverVersion, clientVersion);
    return new VersionCheckResponse(status, serverVersion, Version.RECOMMENDED_VERSION);
  }
}
