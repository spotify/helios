/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.client;

import java.net.InetAddress;
import java.net.URI;

/**
 * A Helios master endpoint which is represented by a {@link URI} and an IP address resolved from
 * the hostname in the URI.
 */
public interface Endpoint {

  /**
   * Returns the {@link URI} of the endpoint.
   * A valid URI for Helios must have a scheme that's either http or https,
   * a hostname, and a port. I.e. it must be of the form http(s)://heliosmaster.domain.net:port.
   * It's up to the implementation to enforce this.
   *
   * @return URI
   */
  URI getUri();

  /**
   * Returns the {@link InetAddress} to which the URI's hostname resolves.
   *
   * @return List of InetAddress
   */
  InetAddress getIp();

}
