/*-
 * -\-\-
 * Helios Testing Library
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.testing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeliosConfig {

  private static final Logger log = LoggerFactory.getLogger(HeliosConfig.class);

  public static final String BASE_CONFIG_FILE = "helios-base.conf";
  public static final String APP_CONFIG_FILE = "helios.conf";

  /**
   * @return The root configuration loaded from the helios configuration files.
   */
  static Config loadConfig() {
    final ConfigResolveOptions resolveOptions = ConfigResolveOptions
        .defaults()
        .setAllowUnresolved(true);

    final ConfigParseOptions parseOptions = ConfigParseOptions.defaults();

    final Config baseConfig = ConfigFactory.load(BASE_CONFIG_FILE, parseOptions, resolveOptions);

    final Config appConfig = ConfigFactory.load(APP_CONFIG_FILE, parseOptions, resolveOptions);

    return appConfig.withFallback(baseConfig);
  }

  /**
   * @param profilePath  The path at which the default profile is specified.
   * @param profilesPath The path at which profiles are nested.
   * @param rootConfig   The config file root to read the default profile from.
   *
   * @return The name of the default profile.
   */
  static Config getDefaultProfile(
      final String profilePath, final String profilesPath, final Config rootConfig) {
    if (!rootConfig.hasPath(profilePath)) {
      log.info("No profile found at " + profilePath + ". Using an empty config.");
      return ConfigFactory.empty();
    }

    return getProfile(profilesPath, rootConfig.getString(profilePath), rootConfig);
  }

  /**
   * @param profilesPath The path at which profiles are nested.
   * @param profile      The name of the profile to load.
   * @param rootConfig   The config file root to load profiles from.
   *
   * @return The requested configuration, or an empty configuration if profile is null.
   */
  static Config getProfile(
      final String profilesPath, final String profile, final Config rootConfig) {
    final String key = profilesPath + profile;
    if (rootConfig.hasPath(key)) {
      log.info("Using configuration profile: " + key);
      return rootConfig.getConfig(key);
    }

    throw new RuntimeException("The configuration profile " + profile + " does not exist");
  }

}
