/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.service;

import java.net.InetSocketAddress;

public class MasterConfig {

  private String site;
  private String endpoint;
  private String zooKeeperConnectString;
  private int muninReporterPort;
  private InetSocketAddress httpEndpoint;

  public int getMuninReporterPort() {
    return muninReporterPort;
  }

  public MasterConfig setMuninReporterPort(int muninReporterPort) {
    this.muninReporterPort = muninReporterPort;
    return this;
  }

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

  public String getZooKeeperConnectString() {
    return zooKeeperConnectString;
  }

  public MasterConfig setZooKeeperConnectString(final String zooKeeperConnectString) {
    this.zooKeeperConnectString = zooKeeperConnectString;
    return this;
  }
}
