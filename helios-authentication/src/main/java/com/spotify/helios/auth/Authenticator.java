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

import com.google.common.base.Optional;

import com.sun.jersey.api.core.HttpRequestContext;

/**
 * Extension of {@link io.dropwizard.auth.Authenticator Dropwizard's Authenticator} interface to
 * add a method that extracts credentials from the HTTP headers.
 */
public interface Authenticator<C> extends io.dropwizard.auth.Authenticator<C, HeliosUser> {

  /**
   * Extract the credentials that the user request is presenting from the sent HTTP headers. This
   * method should not test whether the credentials are valid or not, but simply handle the
   * extraction logic.
   * <p>
   * If no credentials are present in the request, should return an absent Optional.</p>
   */
  Optional<C> extractCredentials(HttpRequestContext request);
}
