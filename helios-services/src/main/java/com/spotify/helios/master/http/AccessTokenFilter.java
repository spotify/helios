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

package com.spotify.helios.master.http;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.net.HttpHeaders;

import com.spotify.helios.authentication.HttpAuthenticator;
import com.spotify.helios.common.PomVersion;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_HEADER;

public class AccessTokenFilter implements Filter {

  private final HttpAuthenticator authenticator;
  private final Predicate<PomVersion> clientVersionRequiresAuth;

  public AccessTokenFilter(HttpAuthenticator authenticator, String versionNumberRequiredForAuth) {
    this.authenticator = authenticator;

    if ("all".equalsIgnoreCase(versionNumberRequiredForAuth)) {
      this.clientVersionRequiresAuth = Predicates.alwaysTrue();
    } else {
      final PomVersion minVersion = PomVersion.parse(versionNumberRequiredForAuth);
      this.clientVersionRequiresAuth = new Predicate<PomVersion>() {
        @Override
        public boolean apply(PomVersion input) {
          // -1 = minVersion is less than input, 0 = equal versions
          // any input version above the minVersion requires auth
          return minVersion.compareTo(input) < 1;
        }
      };
    }
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;

    final String clientVersionHeader = httpRequest.getHeader(HELIOS_VERSION_HEADER);
    if (clientVersionHeader == null) {
      httpResponse.sendError(401, "Requests must include '" + HELIOS_VERSION_HEADER + "' header");
      return;
    }

    final PomVersion clientVersion = PomVersion.parse(clientVersionHeader);
    if (clientVersionRequiresAuth.apply(clientVersion)) {
      final String username = null; // TODO (mbrown): where does this come from?
      final String accessToken = httpRequest.getHeader(authenticator.getHttpAuthHeaderKey());
      if (accessToken == null) {
        httpResponse.addHeader(HttpHeaders.WWW_AUTHENTICATE, authenticator.getSchemeName());
        httpResponse.sendError(401);
      } else if (!authenticator.verifyToken(username, accessToken)) {
        // attach header or response about invalid/expiration?
        httpResponse.addHeader(HttpHeaders.WWW_AUTHENTICATE, authenticator.getSchemeName());
        httpResponse.sendError(401);
      } else {
        // valid!
        chain.doFilter(request, response);
      }
    } else {
      chain.doFilter(request, response);
    }
  }


  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void destroy() {
  }
}
