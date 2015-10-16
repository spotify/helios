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

import java.nio.file.Path;

public class ServerAuthenticationConfig {

  private String enabledScheme;
  private String minimumEnabledVersion;
  private Path pluginsPath;

  public String getEnabledScheme() {
    return enabledScheme;
  }

  public ServerAuthenticationConfig setEnabledScheme(String enabledScheme) {
    this.enabledScheme = enabledScheme;
    return this;
  }

  public String getMinimumEnabledVersion() {
    return minimumEnabledVersion;
  }

  public ServerAuthenticationConfig setMinimumEnabledVersion(String minimumEnabledVersion) {
    this.minimumEnabledVersion = minimumEnabledVersion;
    return this;
  }

  public boolean isEnabledForAllVersions() {
    return this.minimumEnabledVersion == null;
  }

  public Path getPluginsPath() {
    return pluginsPath;
  }

  public ServerAuthenticationConfig setPluginsPath(Path pluginsPath) {
    this.pluginsPath = pluginsPath;
    return this;
  }
}
