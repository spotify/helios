/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.yammer.dropwizard.config.Configuration;

import java.nio.file.Path;
import java.util.Map;

public class AgentConfig extends Configuration {

  private String domain;
  private String name;
  private String zooKeeperConnectionString;
  private String dockerEndpoint;
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

  public AgentConfig setDockerEndpoint(final String endpoint) {
    this.dockerEndpoint = endpoint;
    return this;
  }

  public String getDockerEndpoint() {
    return dockerEndpoint;
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
}
