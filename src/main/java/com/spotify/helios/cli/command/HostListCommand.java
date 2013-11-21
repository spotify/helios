/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HostListCommand extends ControlCommand {

  public HostListCommand(final Subparser parser) {
    super(parser);

    parser.help("list hosts");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out)
      throws ExecutionException, InterruptedException {
    final List<String> agents = client.listAgents().get();
    for (final String agent : agents) {
      out.println(agent);
    }
    return 0;
  }
}
