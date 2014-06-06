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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.Json;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CliConfig {

  private static final String CONFIG_PATH = ".helios" + File.separator + "config";
  public static final List<String> EMPTY_STRING_LIST = Collections.emptyList();
  public static final TypeReference<Map<String, Object>> OBJECT_TYPE =
      new TypeReference<Map<String, Object>>() {};

  private final String username;
  private final List<String> sites;
  private final String srvName;
  private final List<URI> masterEndpoints;

  public CliConfig(List<String> sites, String srvName, List<URI> masterEndpoints) {
    this.username = System.getProperty("user.name");
    this.sites = checkNotNull(sites);
    this.srvName = checkNotNull(srvName);
    this.masterEndpoints = checkNotNull(masterEndpoints);
  }

  public String getUsername() {
    return username;
  }

  public List<String> getSites() {
    return sites;
  }

  public String getSitesString() {
    return Joiner.on(",").join(sites);
  }

  public String getSrvName() {
    return srvName;
  }

  public List<URI> getMasterEndpoints() {
    return masterEndpoints;
  }

  /**
   * Returns a CliConfig instance with values from a config file from under the users home
   * directory:
   *
   * <user.home>/.helios/config
   *
   * If the file is not found, a CliConfig with pre-defined values will be returned.
   *
   * @throws IOException   If the file exists but could not be read
   */
  public static CliConfig fromUserConfig() throws IOException {
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
   * @param defaultsFile The file to parse from
   * @throws IOException   If the file exists but could not be read
   */
  public static CliConfig fromFile(File defaultsFile) throws IOException {
    final Map<String, Object> config;
    // TODO: use typesafe config for config file parsing
    if (defaultsFile.exists() && defaultsFile.canRead()) {
      config = Json.read(Files.readAllBytes(defaultsFile.toPath()), OBJECT_TYPE);
    } else {
      config = ImmutableMap.of();
    }
    return fromMap(config);
  }

  /**
   * Returns a CliConfig instance with values parsed from the specified config node.
   *
   * Any value missing in the config tree will get a pre-defined default value.
   */
  public static CliConfig fromMap(Map<String, Object> config) {
    checkNotNull(config);
    final List<String> sites = getList(config, "sites", EMPTY_STRING_LIST);
    final String srvName = getString(config, "srvName", "helios");
    final List<URI> masterEndpoints = Lists.newArrayList();
    for (final String endpoint : getList(config, "masterEndpoints", EMPTY_STRING_LIST)) {
      masterEndpoints.add(URI.create(endpoint));
    }
    return new CliConfig(sites, srvName, masterEndpoints);
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
