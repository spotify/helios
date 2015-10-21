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

package com.spotify.helios.auth.basic;

import com.google.auto.service.AutoService;

import com.spotify.helios.auth.AuthenticationPlugin;

import io.dropwizard.auth.basic.BasicCredentials;

/** Proof of concept for the authentication plugin framework using HTTP Basic Auth. */
@AutoService(AuthenticationPlugin.class)
public class BasicAuthenticationPlugin implements AuthenticationPlugin<BasicCredentials> {

  @Override
  public String schemeName() {
    return "http-basic";
  }

  @Override
  public ServerAuthentication<BasicCredentials> serverAuthentication() {
    return new BasicServerAuthentication();
  }

  @Override
  public ClientAuthentication<BasicCredentials> clientAuthentication() {
    return null;
  }
}
