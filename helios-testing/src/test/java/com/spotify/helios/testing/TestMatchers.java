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

package com.spotify.helios.testing;


import org.hamcrest.CustomTypeSafeMatcher;

import java.net.InetSocketAddress;

import static java.lang.String.format;


class TestMatchers {

  static CustomTypeSafeMatcher<InetSocketAddress> matchesHostAndPort(final String host,
                                                                      final int port) {
    return new CustomTypeSafeMatcher<InetSocketAddress>(
        format("An InetSocketAddress with host %s and port %d", host, port)) {
      @Override
      protected boolean matchesSafely(final InetSocketAddress inetAddr) {
        return inetAddr.getHostString().equals(host) && inetAddr.getPort() == port;
      }
    };
  }
}
