/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
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
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.cli.Utils.allAsMap;
import static com.spotify.helios.common.descriptors.TaskStatus.State.UNKNOWN;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostJobsCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument quietArg;
  private final Argument fullArg;

  public HostJobsCommand(final Subparser parser) {
    super(parser);

    parser.help("list jobs deployed on a host");

    hostArg = parser.addArgument("host")
        .nargs("+")
        .help("The hosts to list jobs for.");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job id's.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");

  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {

    final List<String> hosts = options.getList(hostArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());
    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Map<String, ListenableFuture<HostStatus>> futures = Maps.newHashMap();
    for (final String host : hosts) {
      futures.put(host, client.hostStatus(host));
    }
    final Map<String, HostStatus> statuses = allAsMap(futures);

    if (quiet) {
      final Set<JobId> sortedUnion = Sets.newTreeSet();
      for (final HostStatus hostStatus : statuses.values()) {
        sortedUnion.addAll(hostStatus.getJobs().keySet());
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
        out.println(Json.asPrettyStringUnchecked(statuses));
      } else {
        final Table table = table(out);
        table.row("HOST", "JOB ID", "NAME", "VERSION", "GOAL", "STATE");
        for (final String host : statuses.keySet()) {
          final HostStatus hostStatus = statuses.get(host);
          if (hostStatus == null) {
            continue;
          }
          final Set<JobId> jobIds = hostStatus.getJobs().keySet();
          final List<JobId> sortedJobIds = natural().sortedCopy(jobIds);
          for (final JobId jobId : sortedJobIds) {
            final Goal goal = hostStatus.getJobs().get(jobId).getGoal();
            final Map<JobId, TaskStatus> taskStatuses = hostStatus.getStatuses();
            final TaskStatus taskStatus = taskStatuses.get(jobId);
            final TaskStatus.State state = taskStatus == null ? UNKNOWN : taskStatus.getState();
            table.row(host, full ? jobId : jobId.toShortString(), jobId.getName(),
                      jobId.getVersion(), goal, state);
          }
        }
        table.print();
      }
    }

    return 0;
  }
}
