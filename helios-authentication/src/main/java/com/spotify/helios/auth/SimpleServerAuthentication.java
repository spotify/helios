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

package com.spotify.helios.auth;

import com.google.common.collect.ImmutableSet;

import com.spotify.helios.auth.AuthenticationPlugin.ServerAuthentication;

import java.util.Set;

import io.dropwizard.jersey.setup.JerseyEnvironment;

/**
 * A base authentication plugin class that can be used if the plugin does not need to add any
 * additional Jersey components.
 */
public abstract class SimpleServerAuthentication<C> implements ServerAuthentication<C> {

  @Override
  public final void registerAdditionalJerseyComponents(final JerseyEnvironment env) {
    // do nothing
  }

  @Override
  public Set<String> unauthenticatedPaths() {
    return ImmutableSet.of();
  }
}
