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

import org.hamcrest.CustomTypeSafeMatcher;

import java.net.InetAddress;
import java.net.URI;
import java.util.Collection;

/** Utilities related to Endpoints for use in other tests. */
public class EndpointsHelper {

  private EndpointsHelper() {
  }

  public static CustomTypeSafeMatcher<URI> matchesAnyEndpoint(
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

}
