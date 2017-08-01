/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CliConfig {

  private static final String HTTPS_SCHEME = "https";
  private static final String HTTP_SCHEME = "http";
  private static final String DOMAIN_SCHEME = "domain";
  private static final String MASTER_ENDPOINTS_KEY = "masterEndpoints";
  private static final String DOMAINS_KEY = "domains";
  private static final String SRV_NAME_KEY = "srvName";
  private static final String CONFIG_DIR = ".helios";
  private static final String CONFIG_FILE = "config";
  private static final String CONFIG_PATH = CONFIG_DIR + File.separator + CONFIG_FILE;
  public static final List<String> EMPTY_STRING_LIST = Collections.emptyList();

  private final String username;
  private final List<String> domains;
  private final String srvName;
  private final List<URI> masterEndpoints;

  public CliConfig(List<String> domains, String srvName, List<URI> masterEndpoints) {
    this.username = System.getProperty("user.name");
    this.domains = checkNotNull(domains);
    this.srvName = checkNotNull(srvName);
    this.masterEndpoints = checkNotNull(masterEndpoints);
  }

  public String getUsername() {
    return username;
  }

  public List<String> getDomains() {
    return domains;
  }

  public String getDomainsString() {
    return Joiner.on(",").join(domains);
  }

  public String getSrvName() {
    return srvName;
  }

  public List<URI> getMasterEndpoints() {
    return masterEndpoints;
  }

  public static String getConfigDirName() {
    return CONFIG_DIR;
  }

  public static String getConfigFileName() {
    return CONFIG_FILE;
  }

  /**
   * Returns a CliConfig instance with values from a config file from under the users home
   * directory:
   *
   * <p>&lt;user.home&gt;/.helios/config
   *
   * <p>If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @return The configuration
   *
   * @throws IOException        If the file exists but could not be read
   * @throws URISyntaxException If a HELIOS_MASTER env var is present and doesn't parse as a URI
   */
  public static CliConfig fromUserConfig(final Map<String, String> environmentVariables)
      throws IOException, URISyntaxException {
    final String userHome = System.getProperty("user.home");
    final String defaults = userHome + File.separator + CONFIG_PATH;
    final File defaultsFile = new File(defaults);
    return fromFile(defaultsFile, environmentVariables);
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified file.
   *
   * <p>If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @param defaultsFile The file to parse from
   *
   * @return The configuration
   *
   * @throws IOException        If the file exists but could not be read
   * @throws URISyntaxException If a HELIOS_MASTER env var is present and doesn't parse as a URI
   */
  public static CliConfig fromFile(final File defaultsFile,
                                   final Map<String, String> environmentVariables)
      throws IOException, URISyntaxException {

    final Config config;
    if (defaultsFile.exists() && defaultsFile.canRead()) {
      config = ConfigFactory.parseFile(defaultsFile);
    } else {
      config = ConfigFactory.empty();
    }
    return fromEnvVar(config, environmentVariables);
  }

  public static CliConfig fromEnvVar(final Config config,
                                     final Map<String, String> environmentVariables)
      throws URISyntaxException {

    final String master = environmentVariables.get("HELIOS_MASTER");
    if (master == null) {
      return fromConfig(config);
    }

    // Specifically only include relevant bits according to the env var setting, rather than
    // strictly overlaying config so that if the config file has a setting for master endpoints, the
    // file doesn't override the env var if it's a domain:// as masterEndpoints takes precedence
    // over domains.
    final URI uri = new URI(master);
    Config configFromEnvVar = ConfigFactory.empty();
    // Always include the srvName bit if it's specified, so it can be specified in the file and
    // a domain flag could be passed on the command line, and it would work as you'd hope.
    if (config.hasPath(SRV_NAME_KEY)) {
      configFromEnvVar = configFromEnvVar.withValue(SRV_NAME_KEY, config.getValue(SRV_NAME_KEY));
    }

    final String scheme = uri.getScheme();
    if (isNullOrEmpty(scheme)) {
      throw new RuntimeException("Your environment variable HELIOS_MASTER=" + master
                                 + " is not a valid URI with a scheme.");
    }

    switch (scheme) {
      case DOMAIN_SCHEME:
        configFromEnvVar = configFromEnvVar.withValue(
            DOMAINS_KEY, ConfigValueFactory.fromIterable(ImmutableList.of(uri.getHost())));
        break;
      case HTTPS_SCHEME:
      case HTTP_SCHEME:
        configFromEnvVar = configFromEnvVar.withValue(
            MASTER_ENDPOINTS_KEY, ConfigValueFactory.fromIterable(ImmutableList.of(master)));
        break;
      default:
        throw new RuntimeException("Your environment variable HELIOS_MASTER=" + master
                                   + " does not have a valid scheme.");
    }

    return fromConfig(configFromEnvVar);
  }

  public static CliConfig fromConfig(final Config config) {
    checkNotNull(config);
    final Map<String, Object> defaultSettings = ImmutableMap.of(
        DOMAINS_KEY, EMPTY_STRING_LIST,
        SRV_NAME_KEY, "helios",
        MASTER_ENDPOINTS_KEY, EMPTY_STRING_LIST
    );
    final Config configWithDefaults = config.withFallback(ConfigFactory.parseMap(defaultSettings));
    final List<String> domains = configWithDefaults.getStringList(DOMAINS_KEY);
    final String srvName = configWithDefaults.getString(SRV_NAME_KEY);
    final List<URI> masterEndpoints = Lists.newArrayList();
    for (final String endpoint : configWithDefaults.getStringList(MASTER_ENDPOINTS_KEY)) {
      masterEndpoints.add(URI.create(endpoint));
    }
    return new CliConfig(domains, srvName, masterEndpoints);
  }
}
