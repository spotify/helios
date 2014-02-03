/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.yammer.dropwizard.config.Configuration;

public class MasterConfig extends Configuration {

  private String site;
  private String endpoint;
  private String zooKeeperConnectString;
  private String name;
  private boolean inhibitMetrics;
  private String statsdHostPort;
  private String riemannHostPort;

  public String getHermesEndpoint() {
    return endpoint;
  }

  public MasterConfig setHermesEndpoint(final String endpoint) {
    this.endpoint = endpoint;
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

  public MasterConfig setRiemannHostPort(String hostPort) {
    this.riemannHostPort = hostPort;
    return this;
  }

  public String getRiemannHostPort() {
    return riemannHostPort;
  }
}
