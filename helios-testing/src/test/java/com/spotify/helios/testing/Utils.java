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


import org.mockito.ArgumentMatcher;

import java.net.InetSocketAddress;

import static org.mockito.Matchers.argThat;


class Utils {

  static InetSocketAddress socketWithHostAndPort(final String host, final int port) {
    return argThat(new ArgumentMatcher<InetSocketAddress>() {
      @Override
      public boolean matches(Object argument) {
        if (argument instanceof InetSocketAddress) {
          final InetSocketAddress inetAddr = (InetSocketAddress) argument;
          if (inetAddr.getHostString().equals(host) && inetAddr.getPort() == port) {
            return true;
          }
        }
        return false;
      }
    });
  }
}
