/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

public class AgentConfig {

  private String site;
  private String name;
  private String zooKeeperConnectionString;
  private int muninReporterPort;
  private String dockerEndpoint;
  private int zooKeeperSessionTimeoutMillis;
  private int zooKeeperConnectionTimeoutMillis;

  public String getName() {
    return name;
  }

  public AgentConfig setName(final String name) {
    this.name = name;
    return this;
  }

  public int getMuninReporterPort() {
    return muninReporterPort;
  }

  public AgentConfig setMuninReporterPort(final int port) {
    this.muninReporterPort = port;
    return this;
  }

  public String getSite() {
    return site;
  }

  public AgentConfig setSite(final String site) {
    this.site = site;
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
}
