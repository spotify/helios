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

import org.apache.commons.codec.binary.Base64;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class BasicAuthProvider implements AuthProvider {

  private final String header;
  private final ListenableFuture<String> headerFuture;

  public BasicAuthProvider(final String username, final String password) {
    // TODO(staffan): Verify that username doesn't contain comma
    final String credentials = username + ":" + password;
    header = "Basic " + Base64.encodeBase64String(credentials.getBytes());
    headerFuture = immediateFuture(header);
  }

  @Override
  public String currentAuthorization() {
    return header;
  }

  @Override
  public ListenableFuture<String> renewAuthorization(final String authHeader) {
    return headerFuture;
  }
}
