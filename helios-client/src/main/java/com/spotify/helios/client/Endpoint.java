/*
 * Copyright (c) 2015 Spotify AB.
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

import java.net.InetAddress;
import java.net.URI;
import java.util.List;

/**
 * A Helios master endpoint which is represented by a URI for the hostname and a list of IP
 * addresses. There can be multiple IP addresses since a hostname can have multiple DNS A records.
 */
public interface Endpoint {

  /**
   * Returns the {@link URI} of the endpoint.
   * @return URI
   */
  URI getUri();

  /**
   * Returns a list of {@link InetAddress} to which the URI's hostname resolves.
   * @return List of InetAddress
   */
  List<InetAddress> getIps();

}
