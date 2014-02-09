/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.base.Objects;

import com.spotify.helios.common.Defaults;
import com.spotify.helios.servicescommon.ServiceParser;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.google.common.base.Throwables.propagate;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class MasterParser extends ServiceParser {

  private final MasterConfig masterConfig;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    final Namespace options = getNamespace();
    final String bindHttp = options.getString("http");
    final InetSocketAddress bindHttpAddress = parseSocketAddress(bindHttp);

    this.masterConfig = new MasterConfig()
        .setHermesEndpoint(options.getString("hm"))
        .setHttpEndpoint(bindHttpAddress)
        .setZooKeeperConnectString(options.getString("zk"))
        .setSite(options.getString("site"))
        .setName(options.getString("name"))
        .setInhibitMetrics(Objects.equal(options.getBoolean("no_metrics"), true))
        .setStatsdHostPort(options.getString("statsd_host_port"));
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    parser.addArgument("--hm")
        .setDefault(Defaults.MASTER_HM_BIND)
        .help("hermes endpoint");

    parser.addArgument("--http")
        .setDefault(Defaults.MASTER_HTTP_BIND)
        .help("http endpoint");

    parser.addArgument("--name")
        .setDefault(getHostName())
        .help("master name");

    parser.addArgument("--no-metrics")
        .setDefault(SUPPRESS)
        .action(storeTrue())
        .help("Turn off all collection and reporting of metrics");

    parser.addArgument("--statsd-host-port")
        .setDefault((String) null)
        .help("host:port of where to send statsd metrics "
            + "(to be useful, --no-metrics must *NOT* be specified)");
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw propagate(e);
    }
  }

  private InetSocketAddress parseSocketAddress(final String addressString) {
    final InetSocketAddress address;
    try {
      final URI u = new URI("http://" + addressString);
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
