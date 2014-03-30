/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.yammer.dropwizard.config.Configuration;

public class MasterConfig extends Configuration {

  private String site;
  private String zooKeeperConnectString;
  private String name;
  private boolean inhibitMetrics;
  private String statsdHostPort;
  private String riemannHostPort;
  private String namelessEndpoint;
  private String sentryDsn;

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

  public MasterConfig setNamelessEndpoint(final String namelessEndpoint) {
    this.namelessEndpoint = namelessEndpoint;
    return this;
  }

  public String getNamelessEndpoint() {
    return namelessEndpoint;
  }

  public String getSentryDsn() {
    return sentryDsn;
  }

  public MasterConfig setSentryDsn(String sentryDsn) {
    this.sentryDsn = sentryDsn;
    return this;
  }
}
