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

package com.spotify.helios.servicescommon;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;

import java.net.URI;

import static com.google.common.base.Optional.fromNullable;
import static java.lang.System.getenv;

/**
 * Represents a dockerd endpoint. A codified DOCKER_HOST.
 */
public class DockerHost {

  public static final int DEFAULT_PORT = 4243;
  public static final String DEFAULT_HOST = "localhost";

  private final String host;
  private final URI uri;
  private final String address;
  private final int port;

  private DockerHost(final String endpoint) {
    final String stripped = endpoint.replaceAll(".*://", "");
    final HostAndPort hostAndPort = HostAndPort.fromString(stripped);
    final String hostText = hostAndPort.getHostText();
    this.port = hostAndPort.getPortOrDefault(defaultPort());
    this.address = Strings.isNullOrEmpty(hostText) ? DEFAULT_HOST : hostText;
    this.host = address + ":" + port;
    this.uri = URI.create("http://" + address + ":" + port);
  }

  /**
   * Get a docker endpoint usable as a DOCKER_HOST environment variable.
   */
  public String host() {
    return host;
  }

  /**
   * Get the docker rest uri.
   */
  public URI uri() {
    return uri;
  }

  /**
   * Get the docker endpoint port.
   */
  public int port() {
    return port;
  }

  /**
   * Get the docker ip address or hostname.
   */
  public String address() {
    return address;
  }

  /**
   * Create a {@link DockerHost} from DOCKER_HOST and DOCKER_PORT env vars.
   */
  public static DockerHost fromEnv() {
    final String host = fromNullable(getenv("DOCKER_HOST")).or(DEFAULT_HOST + ":" + defaultPort());
    return new DockerHost(host);
  }

  /**
   * Create a {@link DockerHost} from an explicit address or uri.
   */
  public static DockerHost from(final String endpoint) {
    return new DockerHost(endpoint);
  }

  private static int defaultPort() {
    final String port = getenv("DOCKER_PORT");
    if (port == null) {
      return DEFAULT_PORT;
    }
    try {
      return Integer.valueOf(port);
    } catch (NumberFormatException e) {
      return DEFAULT_PORT;
    }
  }

  @Override
  public String toString() {
    return host();
  }
}
