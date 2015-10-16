/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.spi.inject.InjectableProvider;

import io.dropwizard.auth.Auth;
import io.dropwizard.jersey.setup.JerseyEnvironment;

/**
 * @param <C> the type of Credentials used
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

  ServerAuthentication<C> serverAuthentication();

  ClientAuthentication<C> clientAuthentication();

  interface ServerAuthentication<C> {

    /**
     * The server-side authentication plugin needs to supply an InjectableProvider to Jersey to
     * allow
     * it to authenticate HTTP requests whenever the resource has an {@link Auth} annotation.
     * <p>
     * Plugin implementations will have to return an InjectableProvider whose
     * <pre>getInjectable</pre> method examines the current HTTP request to extract the
     * Authorization header (or other headers), constructs a "credentials" object out of them, and
     * runs that through an instance of {@link io.dropwizard.auth.Authenticator} to get the
     * HeliosUser instance.
     *
     * @see io.dropwizard.auth.basic.BasicAuthProvider Dropwizard's BasicAuthProvider as an example
     */
    // TODO (mbrown): change this signature to be just the Authenticator and do the common work of
    // the InjectableProvider in this package, as it is mostly boilerplate
    InjectableProvider<Auth, Parameter> authProvider();

    /**
     * A hook for implementations to register additional Jersey components, such as Resource
     * classes
     * for multi-stepped authentication handshakes.
     */
    void registerAdditionalJerseyComponents(JerseyEnvironment env);
  }

  interface ClientAuthentication<C> {
    // TODO (mbrown): have an interface!
  }
}
