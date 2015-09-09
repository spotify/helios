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

package com.spotify.helios.authentication;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

/**
 * A factory for {@link ServerAuthProvider} and {@link ClientAuthProvider} instances
 * and the entry point for authentication plugins.
 * {@link AuthProviderLoader} loads plugins by using {@link java.util.ServiceLoader}
 * to look up {@link AuthProviderFactory} from jar files and class loaders.
 */
public interface AuthProviderFactory {

  /**
   * Create an authenticator. The secret format and semantics are implementation dependent.
   *
   * @param serverName The name of the server using this {@link ServerAuthProvider}
   * @param secret A secret for the authenticator.
   * @return An {@link ServerAuthProvider }
   */
  ServerAuthProvider createServerAuthProvider(String serverName, String secret);

  ClientAuthProvider createClientAuthProvider(Path keyPath, List<URI> authServerUris);
}

