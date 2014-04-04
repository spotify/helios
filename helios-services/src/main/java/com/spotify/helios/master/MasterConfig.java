/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.yammer.dropwizard.config.Configuration;

import java.nio.file.Path;

public class MasterConfig extends Configuration {

  // TODO (dano): defaults

  private String domain;
  private String zooKeeperConnectString;
  private String name;
  private boolean inhibitMetrics;
  private String statsdHostPort;
  private String riemannHostPort;
  private String serviceRegistryAddress;
  private String sentryDsn;
  private Path serviceRegistrarPlugin;
  private int zooKeeperSessionTimeoutMillis;
  private int zooKeeperConnectionTimeoutMillis;

  public String getDomain() {
    return domain;
  }

  public MasterConfig setDomain(final String domain) {
    this.domain = domain;
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

  public MasterConfig setZooKeeperSessionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperSessionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperSessionTimeoutMillis() {
    return zooKeeperSessionTimeoutMillis;
  }

  public MasterConfig setZooKeeperConnectionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperConnectionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperConnectionTimeoutMillis() {
    return zooKeeperConnectionTimeoutMillis;
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

  public MasterConfig setServiceRegistryAddress(final String address) {
    this.serviceRegistryAddress = address;
    return this;
  }

  public String getServiceRegistryAddress() {
    return serviceRegistryAddress ;
  }

  public String getSentryDsn() {
    return sentryDsn;
  }

  public MasterConfig setSentryDsn(String sentryDsn) {
    this.sentryDsn = sentryDsn;
    return this;
  }

  public Path getServiceRegistrarPlugin() {
    return serviceRegistrarPlugin;
  }

  public MasterConfig setServiceRegistrarPlugin(final Path serviceRegistrarPlugin) {
    this.serviceRegistrarPlugin = serviceRegistrarPlugin;
    return this;
  }
}
