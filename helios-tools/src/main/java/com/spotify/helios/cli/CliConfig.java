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

package com.spotify.helios.cli;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.Json;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class CliConfig {

  private static final String HTTP_SCHEME = "http";
  private static final String DOMAIN_SCHEME = "domain";
  private static final String DEPRECATED_SITE_SCHEME = "site";
  private static final String MASTER_ENDPOINTS_KEY = "masterEndpoints";
  private static final String DOMAINS_KEY = "domains";
  private static final String SRV_NAME_KEY = "srvName";
  private static final String CONFIG_DIR = ".helios";
  private static final String CONFIG_FILE = "config";
  private static final String CONFIG_PATH = CONFIG_DIR + File.separator + CONFIG_FILE;
  public static final List<String> EMPTY_STRING_LIST = Collections.emptyList();
  public static final TypeReference<Map<String, Object>> OBJECT_TYPE =
      new TypeReference<Map<String, Object>>() {};

  static Map<String, String> environment = System.getenv();

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
   * &lt;user.home&gt;/.helios/config
   *
   * If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @throws IOException        If the file exists but could not be read
   * @throws URISyntaxException If a HELIOS_MASTER env var is present and doesn't parse as a URI
   */
  public static CliConfig fromUserConfig() throws IOException, URISyntaxException {
    final String userHome = System.getProperty("user.home");
    final String defaults = userHome + File.separator + CONFIG_PATH;
    final File defaultsFile = new File(defaults);
    return fromFile(defaultsFile);
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified file.
   *
   * If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @param defaultsFile        The file to parse from
   * @throws IOException        If the file exists but could not be read
   * @throws URISyntaxException If a HELIOS_MASTER env var is present and doesn't parse as a URI
   */
  public static CliConfig fromFile(File defaultsFile) throws IOException, URISyntaxException {
    final Map<String, Object> config;
    // TODO: use typesafe config for config file parsing
    if (defaultsFile.exists() && defaultsFile.canRead()) {
      config = Json.read(Files.readAllBytes(defaultsFile.toPath()), OBJECT_TYPE);
    } else {
      config = ImmutableMap.of();
    }
    return fromEnvVar(config);
  }

  public static CliConfig fromEnvVar(Map<String, Object> config) throws URISyntaxException {
    final String master = environment.get("HELIOS_MASTER");
    if (master == null) {
      return fromMap(config);
    }

    // Specifically only include relevant bits according to the env var setting, rather than
    // strictly overlaying config so that if the config file has a setting for master endpoints, the
    // file doesn't override the env var if it's a domain:// as masterEndpoints takes precedence
    // over domains.
    final URI uri = new URI(master);
    final Builder<String, Object> builder = ImmutableMap.<String, Object>builder();
    // Always include the srvName bit if it's specified, so it can be specified in the file and
    // a domain flag could be passed on the command line, and it would work as you'd hope.
    if (config.containsKey(SRV_NAME_KEY)) {
      builder.put(SRV_NAME_KEY, config.get(SRV_NAME_KEY));
    }

    // TODO (dxia) Remove DEPRECATED_SITE_SCHEME after a period of time
    final String scheme = uri.getScheme();
    if (isNullOrEmpty(scheme)) {
      throw new RuntimeException("Your environment variable HELIOS_MASTER=" + master +
                                 " is not a valid URI with a scheme.");
    }

    switch (scheme) {
      case DOMAIN_SCHEME:
      case DEPRECATED_SITE_SCHEME:
        builder.put(DOMAINS_KEY, ImmutableList.of(uri.getHost())).build();
        break;
      case HTTP_SCHEME:
        builder.put(MASTER_ENDPOINTS_KEY, ImmutableList.of(master));
        break;
      default:
        throw new RuntimeException("Your environment variable HELIOS_MASTER=" + master +
                                   " does not have a valid scheme.");
    }

    return fromMap(builder.build());
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified config node.
   *
   * Any value missing in the config tree will get a pre-defined default value.
   */
  public static CliConfig fromMap(Map<String, Object> config) {
    checkNotNull(config);
    final List<String> domains = getList(config, DOMAINS_KEY, EMPTY_STRING_LIST);
    final String srvName = getString(config, SRV_NAME_KEY, "helios");
    final List<URI> masterEndpoints = Lists.newArrayList();
    for (final String endpoint : getList(config, MASTER_ENDPOINTS_KEY, EMPTY_STRING_LIST)) {
      masterEndpoints.add(URI.create(endpoint));
    }
    return new CliConfig(domains, srvName, masterEndpoints);
  }

  private static String getString(final Map<String, Object> config, final String key,
                                  final String defaultValue) {
    return Optional.fromNullable((String) config.get(key)).or(defaultValue);
  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> getList(final Map<String, Object> config, final String key,
                                     final List<T> defaultValue) {
    final List<T> value = (List<T>) config.get(key);
    return Optional.fromNullable(value).or(defaultValue);
  }
}
