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

package com.spotify.helios.client;

import org.apache.http.HttpHost;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;

import java.net.InetAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;

/** Utilities related to Endpoints for use in other tests. */
public class EndpointsHelper {

  private EndpointsHelper() {
  }


  /**
   * A URI matcher where uri.host must equal the InetAddress of one of the given endpoints (the
   * scheme must also match) and the path portion of the URI must match the argument.
   */
  static Matcher<URI> matchesAnyEndpoint(
      final Collection<Endpoint> endpoints,
      final String path) {

    return new CustomTypeSafeMatcher<URI>("A URI matching one of the endpoints in " + endpoints) {
      @Override
      protected boolean matchesSafely(final URI item) {
        for (Endpoint endpoint : endpoints) {
          final InetAddress ip = endpoint.getIp();
          final URI uri = endpoint.getUri();

          if (item.getScheme().equals(uri.getScheme()) &&
              item.getHost().equals(ip.getHostAddress()) &&
              item.getPath().equals(path)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  /** Matches a HttpHost where the InetAddress matches one of the given endpoints. */
  static Matcher<HttpHost> hostFor(final List<Endpoint> endpoints) {
    final String description = "A HttpHost matching one of the endpoints in: " + endpoints;
    return new CustomTypeSafeMatcher<HttpHost>(description) {
      @Override
      protected boolean matchesSafely(final HttpHost item) {
        for (Endpoint endpoint : endpoints) {
          final URI uri = endpoint.getUri();
          if (item.getAddress().equals(endpoint.getIp()) &&
              item.getSchemeName().equals(uri.getScheme()) &&
              item.getPort() == uri.getPort()) {
            return true;
          }
        }
        return false;
      }
    };
  }

}
