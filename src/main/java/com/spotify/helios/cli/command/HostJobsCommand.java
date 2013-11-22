/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Ordering.natural;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostJobsCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument quietArg;

  public HostJobsCommand(final Subparser parser) {
    super(parser);

    parser.help("list jobs deployed on a host");

    hostArg = parser.addArgument("host")
        .action(append())
        .setDefault(Lists.newArrayList())
        .help("The hosts to list jobs for.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");

  }

  @Override
  int run(final Namespace options, final Client client, final PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {

    final List<String> hosts = options.getList(hostArg.getDest());
    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Map<String, AgentStatus> hostStatuses = Maps.newHashMap();

    for (final String host : hosts) {
      final AgentStatus agentStatus = client.agentStatus(host).get();
      hostStatuses.put(host, agentStatus);
    }

    if (quiet) {
      final Set<JobId> sortedUnion = Sets.newTreeSet();
      for (final AgentStatus agentStatus : hostStatuses.values()) {
        sortedUnion.addAll(agentStatus.getJobs().keySet());
      }
      if (json) {
        out.println(Json.asPrettyStringUnchecked(sortedUnion));
      } else {
        for (final JobId jobId : sortedUnion) {
          out.println(jobId);
        }
      }
    } else {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(hostStatuses));
      } else {
        final Table table = new Table(out);
        table.row("HOST", "JOB ID", "NAME", "VERSION", "GOAL", "STATE");
        for (final String host : hostStatuses.keySet()) {
          final AgentStatus agentStatus = hostStatuses.get(host);
          final Set<JobId> jobIds = agentStatus.getJobs().keySet();
          final List<JobId> sortedJobIds = natural().sortedCopy(jobIds);
          for (final JobId jobId : sortedJobIds) {
            final Goal goal = agentStatus.getJobs().get(jobId).getGoal();
            final TaskStatus.State state = agentStatus.getStatuses().get(jobId).getState();
            table.row(host, jobId, jobId.getName(), jobId.getVersion(), goal, state);
          }
        }
        table.print();
      }
    }

    return 0;
  }
}
