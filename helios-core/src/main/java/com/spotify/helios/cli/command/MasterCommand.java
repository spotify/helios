/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.Entrypoint;
import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.common.Defaults;
import com.spotify.helios.service.MasterConfig;
import com.spotify.helios.service.MasterService;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Starts the helios master
 */
public class MasterCommand extends Command {

  private final Argument siteArg;
  private final Argument bindHermesArg;
  private final Argument bindHttpArg;
  private final Argument zookeeperConnectionArg;
  private final Argument muninPortArg;

  public MasterCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    siteArg = parser.addArgument("-s", "--site")
        .setDefault(Defaults.SITES.get(0))
        .help("backend site");

    bindHermesArg = parser.addArgument("--hm")
        .setDefault(Defaults.MASTER_HM_BIND)
        .help("hermes endpoint");

    bindHttpArg = parser.addArgument("--http")
        .setDefault(Defaults.MASTER_HTTP_BIND)
        .help("http endpoint");

    zookeeperConnectionArg = parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    muninPortArg = parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4951)
        .help("munin port (0 = disabled)");
  }

  @Override
  public Entrypoint getEntrypoint(Namespace options) {
    final String controlHttp = options.getString(bindHttpArg.getDest());
    final InetSocketAddress controlHttpAddress = parseSocketAddress(controlHttp);

    final MasterConfig config = new MasterConfig()
        .setControlEndpoint(options.getString(bindHermesArg.getDest()))
        .setControlHttpEndpoint(controlHttpAddress)
        .setZooKeeperConnectString(options.getString(zookeeperConnectionArg.getDest()))
        .setSite(options.getString(siteArg.getDest()))
        .setMuninReporterPort(options.getInt(muninPortArg.getDest()));

    return new ServiceEntrypoint(new MasterService(config));
  }

  private InetSocketAddress parseSocketAddress(final String controlHttp) {
    final InetSocketAddress controlHttpAddress;
    try {
      final URI u = new URI("tcp://" + controlHttp);
      controlHttpAddress = new InetSocketAddress(u.getHost(), u.getPort());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad address: " + controlHttp, e);
    }
    return controlHttpAddress;
  }
}
