/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import java.net.InetSocketAddress;

public class MasterConfig {

  private String site;
  private String endpoint;
  private String zooKeeperConnectString;
  private String name;
  private InetSocketAddress httpEndpoint;
  private boolean inhibitMetrics;
  private String statsdHostPort;

  public String getHermesEndpoint() {
    return endpoint;
  }

  public MasterConfig setHermesEndpoint(final String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  public InetSocketAddress getHttpEndpoint() {
    return httpEndpoint;
  }

  public MasterConfig setHttpEndpoint(final InetSocketAddress endpoint) {
    this.httpEndpoint = endpoint;
    return this;
  }

  public String getSite() {
    return site;
  }

  public MasterConfig setSite(final String site) {
    this.site = site;
    return this;
  }

  public MasterConfig setName(final String name) {
    this.name = name;
    return this;
  }

  public String getName() {
    return name;
  }

  public String getZooKeeperConnectString() {
    return zooKeeperConnectString;
  }

  public MasterConfig setZooKeeperConnectString(final String zooKeeperConnectString) {
    this.zooKeeperConnectString = zooKeeperConnectString;
    return this;
  }

  public MasterConfig setInhibitMetrics(boolean inhibit) {
    this.inhibitMetrics = inhibit;
    return this;
  }

  public boolean isInhibitMetrics() {
    return inhibitMetrics;
  }

  public MasterConfig setStatsdHostPort(String hostPort) {
    this.statsdHostPort = hostPort;
    return this;
  }

  public String getStatsdHostPort() {
    return statsdHostPort;
  }
}
