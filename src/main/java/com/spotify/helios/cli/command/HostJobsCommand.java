/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableSortedMap;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.JobId;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class HostJobsCommand extends ControlCommand {

  private final Argument hostArg;

  public HostJobsCommand(final Subparser parser) {
    super(parser);

    hostArg = parser.addArgument("host")
        .help("The host to list jobs for.");
  }

  @Override
  int run(final Namespace options, final Client client, final PrintStream out)
      throws ExecutionException, InterruptedException {
    final String host = options.getString(hostArg.getDest());
    final AgentStatus agentStatus = client.agentStatus(host).get();
    final ImmutableSortedMap<JobId, Deployment>
        sortedJobs = ImmutableSortedMap.copyOf(agentStatus.getJobs());

    for (final Map.Entry<JobId, Deployment> entry : sortedJobs.entrySet()) {
      final Deployment job = entry.getValue();
      out.printf("%s: %s %s%n", entry.getKey(), job.getJobId(), job.getGoal());
    }

    return 0;
  }
}
