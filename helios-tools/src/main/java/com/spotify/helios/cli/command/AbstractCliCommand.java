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

package com.spotify.helios.cli.command;

import com.google.common.base.Optional;

import com.spotify.helios.auth.AuthProvider;
import com.spotify.helios.cli.Target;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.client.HeliosClient.Builder;

import java.io.PrintStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;

abstract class AbstractCliCommand implements CliCommand {

  HeliosClient getClient(final Target target, final PrintStream err,
                         final String username,
                         final Optional<String> eagerAuthenticationScheme,
                         final AuthProvider.Factory authProviderFactory) {

    List<URI> endpoints = Collections.emptyList();
    try {
      endpoints = target.getEndpointSupplier().get();
    } catch (Exception ignore) {
      // TODO (dano): Nasty. Refactor target to propagate resolution failure in a checked manner.
    }
    if (endpoints.size() == 0) {
      err.println("Failed to resolve helios master in " + target);
      return null;
    }

    final Builder builder = HeliosClient.newBuilder()
        .setEndpointSupplier(target.getEndpointSupplier())
        .setUser(username)
        .setAuthProviderFactory(authProviderFactory);

    if (eagerAuthenticationScheme.isPresent()) {
      builder.setEagerAuthenticationScheme(eagerAuthenticationScheme.get());
    }

    return builder.build();
  }

}
