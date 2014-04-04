/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.servicescommon.ServiceParser;
import com.yammer.dropwizard.config.HttpConfiguration;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;

public class MasterParser extends ServiceParser {

  private final MasterConfig masterConfig;

  private Argument httpArg;
  private Argument adminArg;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    final Namespace options = getNamespace();
    final InetSocketAddress httpAddress = parseSocketAddress(options.getString(httpArg.getDest()));

    final MasterConfig config = new MasterConfig()
        .setZooKeeperConnectString(getZooKeeperConnectString())
        .setZooKeeperSessionTimeoutMillis(getZooKeeperSessionTimeoutMillis())
        .setZooKeeperConnectionTimeoutMillis(getZooKeeperConnectionTimeoutMillis())
        .setDomain(getDomain())
        .setName(getName())
        .setStatsdHostPort(getStatsdHostPort())
        .setRiemannHostPort(getRiemannHostPort())
        .setInhibitMetrics(getInhibitMetrics())
        .setSentryDsn(getSentryDsn())
        .setServiceRegistryAddress(getServiceRegistryAddress())
        .setServiceRegistrarPlugin(getServiceRegistrarPlugin());

    final HttpConfiguration http = config.getHttpConfiguration();
    http.setPort(httpAddress.getPort());
    http.setBindHost(httpAddress.getHostString());
    http.setAdminPort(options.getInt(adminArg.getDest()));

    this.masterConfig = config;
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    httpArg = parser.addArgument("--http")
        .setDefault("http://0.0.0.0:5801")
        .help("http endpoint");

    adminArg = parser.addArgument("--admin")
        .type(Integer.class)
        .setDefault(5802)
        .help("admin http port");
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }
}
