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

import com.google.common.base.Preconditions;

import java.nio.file.Path;

import javax.annotation.Nonnull;

public class ServerAuthenticationConfig {

  private String enabledScheme;
  private String minimumRequiredVersion;
  private Path pluginPath;

  @Nonnull
  public String getEnabledScheme() {
    return enabledScheme;
  }

  public ServerAuthenticationConfig setEnabledScheme(String enabledScheme) {
    this.enabledScheme = Preconditions.checkNotNull(enabledScheme);
    return this;
  }

  public String getMinimumRequiredVersion() {
    return minimumRequiredVersion;
  }

  public ServerAuthenticationConfig setMinimumRequiredVersion(String minimumRequiredVersion) {
    this.minimumRequiredVersion = minimumRequiredVersion;
    return this;
  }

  public boolean isEnabledForAllVersions() {
    return this.minimumRequiredVersion == null;
  }

  public Path getPluginPath() {
    return pluginPath;
  }

  public ServerAuthenticationConfig setPluginPath(Path pluginPath) {
    this.pluginPath = pluginPath;
    return this;
  }
}
