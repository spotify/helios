/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableSortedMap;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;
import com.spotify.helios.service.descriptors.AgentJob;
import com.spotify.helios.service.descriptors.AgentStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class HostJobsCommand extends ControlCommand {

  private final Argument hostArg;

  public HostJobsCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    hostArg = parser.addArgument("host")
        .help("The host to list jobs for.");
  }

  @Override
  int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {
    final String host = options.getString(hostArg.getDest());
    final AgentStatus agentStatus = client.agentStatus(host).get();
    final Map<String, AgentJob> sortedJobs = ImmutableSortedMap.copyOf(agentStatus.getJobs());

    for (final Map.Entry<String, AgentJob> entry : sortedJobs.entrySet()) {
      final AgentJob job = entry.getValue();
      out.printf("%s: %s %s%n", entry.getKey(), job.getJob(), job.getGoal());
    }

    return 0;
  }
}
