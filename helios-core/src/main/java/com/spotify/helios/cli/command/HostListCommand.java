/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HostListCommand extends ControlCommand {

  public HostListCommand(final Subparser parser,
                         final CliConfig cliConfig,
                         final PrintStream out) {
    super(parser, cliConfig, out);
  }

  @Override
  int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {
    final List<String> agents = client.listAgents().get();
    for (final String agent : agents) {
      out.println(agent);
    }
    return 0;
  }
}
