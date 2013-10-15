/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.Entrypoint;
import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.common.Defaults;
import com.spotify.helios.service.AgentConfig;
import com.spotify.helios.service.AgentService;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.google.common.base.Throwables.propagate;

public class AgentCommand extends Command {

  private final Argument siteArg;
  private final Argument zookeeperConnectionArg;
  private final Argument nameArg;
  private final Argument muninPortArg;
  private final Argument dockerEndpointArg;

  public AgentCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    nameArg = parser.addArgument("--name")
        .setDefault(getHostName())
        .help("agent name");

    siteArg = parser.addArgument("-s", "--site")
        .setDefault(Defaults.SITES.get(0))
        .help("backend site");

    zookeeperConnectionArg = parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    muninPortArg = parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4952)
        .help("munin port (0 = disabled)");

    dockerEndpointArg = parser.addArgument("--docker")
        .setDefault("http://localhost:4160")
        .help("docker endpoint");
  }

  private String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw propagate(e);
    }
  }

  @Override
  public Entrypoint getEntrypoint(final Namespace options) {
    final String uriString = options.getString(dockerEndpointArg.getDest());
    try {
      new URI(uriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad docker endpoint: " + uriString, e);
    }

    final String name = options.getString(nameArg.getDest());

    final AgentConfig config = new AgentConfig()
        .setName(name)
        .setZooKeeperConnectionString(options.getString(zookeeperConnectionArg.getDest()))
        .setSite(options.getString(siteArg.getDest()))
        .setMuninReporterPort(options.getInt(muninPortArg.getDest()))
        .setDockerEndpoint(options.getString(dockerEndpointArg.getDest()));

    return new ServiceEntrypoint(new AgentService(config));
  }

}

