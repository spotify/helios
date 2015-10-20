/*
 * Copyright (c) 2015 Spotify AB.
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

import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.transport.RequestDispatcher;

public interface AuthProvider {

  /**
   * Get the current value to use in the "Authorization" header.
   *
   * @return {@code null} if the value is unavailable.
   */
  String currentAuthorization();

  /**
   * This method is called when current credentials are unknown or invalid (e.g. expired).
   * Implementations should renew the authorization if supported when this method is called.
   * If renewing credentials is unsupported an implementation MUST return an immediate future
   * with the current credentials.
   */
  ListenableFuture<String> renewAuthorization(String authHeader);

  public interface Factory {

    AuthProvider create(RequestDispatcher requestDispatcher);
  }
}
