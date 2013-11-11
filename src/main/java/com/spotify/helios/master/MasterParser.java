/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.Defaults;
import com.spotify.helios.common.ServiceParser;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

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
        .setMuninReporterPort(options.getInt("munin_port"));
  }

  protected void addArgs(final ArgumentParser parser) {
    parser.addArgument("--hm")
        .setDefault(Defaults.MASTER_HM_BIND)
        .help("hermes endpoint");

    parser.addArgument("--http")
        .setDefault(Defaults.MASTER_HTTP_BIND)
        .help("http endpoint");

    parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4951)
        .help("munin port (0 = disabled)");
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
