/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.servicescommon.ServiceParser;
import com.yammer.dropwizard.config.HttpConfiguration;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.base.Optional.fromNullable;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class MasterParser extends ServiceParser {

  private final MasterConfig masterConfig;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    final Namespace options = getNamespace();
    final InetSocketAddress httpAddress = parseSocketAddress(options.getString("http"));

    final MasterConfig config = new MasterConfig()
        .setZooKeeperConnectString(options.getString("zk"))
        .setSite(options.getString("site"))
        .setName(options.getString("name"))
        .setStatsdHostPort(options.getString("statsd_host_port"))
        .setRiemannHostPort(options.getString("riemann_host_port"))
        .setInhibitMetrics(fromNullable(options.getBoolean("no_metrics")).or(false));

    final HttpConfiguration http = config.getHttpConfiguration();
    http.setPort(httpAddress.getPort());
    http.setBindHost(httpAddress.getHostString());
    http.setAdminPort(options.getInt("admin"));

    this.masterConfig = config;
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    parser.addArgument("--http")
        .setDefault("http://0.0.0.0:5801")
        .help("http endpoint");

    parser.addArgument("--admin")
        .type(Integer.class)
        .setDefault(5802)
        .help("admin http port");

    parser.addArgument("--no-metrics")
        .setDefault(SUPPRESS)
        .action(storeTrue())
        .help("Turn off all collection and reporting of metrics");

    parser.addArgument("--statsd-host-port")
        .setDefault((String) null)
        .help("host:port of where to send statsd metrics "
            + "(to be useful, --no-metrics must *NOT* be specified)");

    parser.addArgument("--riemann-host-port")
        .setDefault((String) null)
        .help("host:port of where to send riemann events and metrics "
            + "(to be useful, --no-metrics must *NOT* be specified)");
  }

  private InetSocketAddress parseSocketAddress(final String addressString) {
    final InetSocketAddress address;
    try {
      final URI u = new URI(addressString);
      address = new InetSocketAddress(u.getHost(), u.getPort());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad address: " + addressString, e);
    }
    return address;
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }
}
