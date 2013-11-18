/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Lists;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.append;

public class HostStatusCommand extends ControlCommand {

  private final Argument hostsArg;

  public HostStatusCommand(final Subparser parser) {
    super(parser);

    hostsArg = parser.addArgument("hosts")
        .action(append())
        .setDefault(Lists.newArrayList())
        .help("");
  }

  @Override
  int run(final Namespace options, final Client client, final PrintStream out)
      throws ExecutionException, InterruptedException {

    List<String> hosts = options.getList(hostsArg.getDest());

    if (hosts.isEmpty()) {
      hosts = client.listAgents().get();
    }

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final AgentStatus hostStatus = client.agentStatus(host).get();
      if (hostStatus != null) {
        out.printf("%s%n", hostStatus);
      } else {
        out.printf("-%n");
      }
    }

    return 0;
  }
}
