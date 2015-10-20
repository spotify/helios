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

import java.util.Map;
import java.util.Set;

import io.dropwizard.jersey.setup.JerseyEnvironment;

/**
 * To add new authentication schemes to Helios, a plugin needs to implement this interface to
 * provide the logic for client-side and server-side authentication.
 *
 * @param <C> the type of Credentials used
 * @see com.spotify.helios.auth.basic.BasicAuthenticationPlugin an example implementation providing
 * HTTP Basic authentication
 */
public interface AuthenticationPlugin<C> {

  /**
   * The name of the scheme that this plugin provides.
   * <p>
   * When the Helios master starts up and attempts to load all configured authentication plugins,
   * it will compare the return value of this method against the <code>--auth-scheme</code>
   * argument that it was started with.
   * </p>
   */
  String schemeName();

  /**
   * Create the ServerAuthentication instance to use with this plugin.
   *
   * @param environment the environment variables that the Helios Master was started with. Provided
   *                    as a method parameter rather than System.getenv() for ease of testing.
   */
  ServerAuthentication<C> serverAuthentication(Map<String, String> environment);

  /**
   * The server-side half of authentication in Helios. Responsible for creating {@link
   * Authenticator} instances which translate HTTP headers into credentials and validate them.
   */
  interface ServerAuthentication<C> {

    /**
     * An Authenticator instance to use when authenticating HTTP requests to Helios.
     * <p>
     * Helios' Authentication support builds on top of the {@link io.dropwizard.auth.Authenticator
     * Authenticator interface from Dropwizard} to add in a method for transforming HTTP headers
     * into a "credentials" object. The latter is then fed into the {@link
     * Authenticator#authenticate(Object)} method (defined in the dropwizard Authenticator
     * interface) to actually authenticate the request.
     * </p>
     */
    Authenticator<C> authenticator();

    /**
     * A hook for implementations to register additional Jersey components, such as Resource
     * classes
     * for multi-stepped authentication handshakes.
     */
    void registerAdditionalJerseyComponents(JerseyEnvironment env);

    /**
     * Returns a Set of URI paths (such as <code>auth</code>) that should not require
     * authentication. Note that the path as presented by Jersey will not have the leading
     * <code>/</code>.
     * <p>
     * If the plugin registers any endpoints for authentication handshakes in {@link
     * #registerAdditionalJerseyComponents(JerseyEnvironment)} then it must also return the path(s)
     * to those endpoint(s) in this method.</p>
     */
    Set<String> unauthenticatedPaths();
  }
}
