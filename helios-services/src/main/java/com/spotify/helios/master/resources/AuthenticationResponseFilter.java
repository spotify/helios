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

package com.spotify.helios.master.resources;

import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

/**
 * If user is authenticated, add a response header containing the username.
 * <p>
 * Possibly only useful for debugging/troubleshooting the authentication module.</p>
 */
public class AuthenticationResponseFilter implements ContainerResponseFilter {

  public static final String USER_PROPERTY_KEY = "helios.user.principal";

  @Override
  public ContainerResponse filter(final ContainerRequest request,
                                  final ContainerResponse response) {

    if (request.getProperties().containsKey(USER_PROPERTY_KEY)) {
      final HeliosUser user = (HeliosUser) request.getProperties().get(USER_PROPERTY_KEY);
      response.getHttpHeaders().putSingle("Authentication-Principal", user.getUsername());
    }

    return response;
  }
}
