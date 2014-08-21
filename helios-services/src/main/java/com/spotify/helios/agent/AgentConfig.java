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

package com.spotify.helios.agent;

import com.spotify.helios.servicescommon.DockerHost;
import com.yammer.dropwizard.config.Configuration;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * The configuration of the Helios agent.
 */
public class AgentConfig extends Configuration {

  private String domain;
  private String name;
  private String zooKeeperConnectionString;
  private DockerHost dockerHost;
  private int zooKeeperSessionTimeoutMillis;
  private int zooKeeperConnectionTimeoutMillis;
  private Map<String, String> envVars;
  private String redirectToSyslog;
  private boolean inhibitMetrics;
  private Path stateDirectory;
  private String statsdHostPort;
  private String riemannHostPort;
  private String serviceRegistryAddress;
  private int portRangeStart;
  private int portRangeEnd;
  private String sentryDsn;
  private Path serviceRegistrarPlugin;
  private String id;
  private List<String> dns;

  public boolean isInhibitMetrics() {
    return inhibitMetrics;
  }

  public AgentConfig setInhibitMetrics(boolean inhibitMetrics) {
    this.inhibitMetrics = inhibitMetrics;
    return this;
  }

  public String getName() {
    return name;
  }

  public AgentConfig setName(final String name) {
    this.name = name;
    return this;
  }

  public AgentConfig setRedirectToSyslog(final String redirect) {
    this.redirectToSyslog = redirect;
    return this;
  }

  public String getRedirectToSyslog() {
    return redirectToSyslog;
  }

  public String getDomain() {
    return domain;
  }

  public AgentConfig setDomain(final String domain) {
    this.domain = domain;
    return this;
  }

  public String getZooKeeperConnectionString() {
    return zooKeeperConnectionString;
  }

  public AgentConfig setZooKeeperConnectionString(final String connectionString) {
    this.zooKeeperConnectionString = connectionString;
    return this;
  }

  public AgentConfig setDockerHost(final DockerHost dockerHost) {
    this.dockerHost = dockerHost;
    return this;
  }

  public DockerHost getDockerHost() {
    return dockerHost;
  }

  public AgentConfig setZooKeeperSessionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperSessionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperSessionTimeoutMillis() {
    return zooKeeperSessionTimeoutMillis;
  }

  public AgentConfig setZooKeeperConnectionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperConnectionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperConnectionTimeoutMillis() {
    return zooKeeperConnectionTimeoutMillis;
  }

  public AgentConfig setEnvVars(final Map<String, String> envVars) {
    this.envVars = envVars;
    return this;
  }

  public Map<String, String> getEnvVars() {
    return envVars;
  }

  public AgentConfig setStateDirectory(final Path stateDirectory) {
    this.stateDirectory = stateDirectory;
    return this;
  }

  public Path getStateDirectory() {
    return stateDirectory;
  }

  public AgentConfig setStatsdHostPort(String hostPort) {
    this.statsdHostPort = hostPort;
    return this;
  }

  public String getStatsdHostPort() {
    return statsdHostPort;
  }
  public AgentConfig setRiemannHostPort(String hostPort) {
    this.riemannHostPort = hostPort;
    return this;
  }

  public String getRiemannHostPort() {
    return riemannHostPort;
  }

  public AgentConfig setServiceRegistryAddress(final String address) {
    this.serviceRegistryAddress = address;
    return this;
  }

  public String getServiceRegistryAddress() {
    return serviceRegistryAddress;
  }

  public AgentConfig setPortRange(final int start, final int end) {
    this.portRangeStart = start;
    this.portRangeEnd = end;
    return this;
  }

  public int getPortRangeStart() {
    return portRangeStart;
  }

  public int getPortRangeEnd() {
    return portRangeEnd;
  }

  public String getSentryDsn() {
    return sentryDsn;
  }

  public AgentConfig setSentryDsn(String sentryDsn) {
    this.sentryDsn = sentryDsn;
    return this;
  }

  public Path getServiceRegistrarPlugin() {
    return serviceRegistrarPlugin;
  }

  public AgentConfig setServiceRegistrarPlugin(final Path serviceRegistrarPlugin) {
    this.serviceRegistrarPlugin = serviceRegistrarPlugin;
    return this;
  }

  public String getId() {
    return id;
  }

  public AgentConfig setId(final String id) {
    this.id = id;
    return this;
  }

  public List<String> getDns() {
    return dns;
  }

  public AgentConfig setDns(List<String> dns) {
    this.dns = dns;
    return this;
  }
}
