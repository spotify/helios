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

package com.spotify.helios.master.http;

import static com.spotify.helios.common.Version.POM_VERSION;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_SERVER_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_STATUS_HEADER;
import static com.spotify.helios.common.VersionCompatibility.Status.INVALID;
import static com.spotify.helios.common.VersionCompatibility.Status.MISSING;
import static com.spotify.helios.common.VersionCompatibility.getStatus;

import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.VersionCompatibility.Status;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks if the client passed a Helios-Version: header, and does a version compatibility check
 * against the server version, and sets a Helios-Version-Status: header with the status.  Also
 * sets a Helios-Server-Version: header with the server version (what else really?).
 */
public class VersionResponseFilter implements Filter {
  private static final Logger log = LoggerFactory.getLogger(VersionResponseFilter.class);

  private static final PomVersion SERVER_VERSION = PomVersion.parse(POM_VERSION);

  private final MasterMetrics metrics;

  public VersionResponseFilter(MasterMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // If ever there was a valid use of goto, this would be it... if Java had it.
    if (!(request instanceof HttpServletRequest)) {
      log.debug("request is not HTTP");
      chain.doFilter(request, response);
      return;
    }

    final HttpServletRequest httpReq = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;
    httpResponse.addHeader(HELIOS_SERVER_VERSION_HEADER, SERVER_VERSION.toString());

    final String header = httpReq.getHeader(HELIOS_VERSION_HEADER);
    if (header == null) {
      log.debug("No header " + HELIOS_VERSION_HEADER);
      httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, MISSING.toString());
      chain.doFilter(request, response);
      return;
    }

    final PomVersion clientVersion;
    try {
      clientVersion = PomVersion.parse(header);
    } catch (RuntimeException e) {
      log.debug("failure to parse version header " + header);
      httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, INVALID.toString());
      httpResponse.sendError(400, "Helios client version format is bogus - expect n.n.n");
      return;
    }

    metrics.clientVersion(clientVersion.toString());

    final Status status = getStatus(SERVER_VERSION, clientVersion);
    httpResponse.addHeader(HELIOS_VERSION_STATUS_HEADER, status.toString());
    if (status == Status.INCOMPATIBLE) {
      log.debug("version " + clientVersion + " is incompatible");
      httpResponse.sendError(426, "Your client version is incompatible with the server version "
                                  + POM_VERSION);
    } else {
      log.debug("version " + clientVersion + " state " + status);
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig arg0) throws ServletException {}

  @Override
  public void destroy() {}
}
