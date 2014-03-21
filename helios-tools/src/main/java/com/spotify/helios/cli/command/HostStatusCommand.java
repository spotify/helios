/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.JobStatusTable;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.isNull;
import static com.google.common.base.Predicates.not;
import static com.spotify.helios.cli.Output.jobStatusTable;
import static com.spotify.helios.cli.Utils.allAsMap;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostStatusCommand extends ControlCommand {

  private final Argument hostsArg;
  private final Argument fullArg;

  public HostStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show host status");

    hostsArg = parser.addArgument("hosts")
        .nargs("*")
        .help("");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job and container id's.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json)
      throws ExecutionException, InterruptedException {

    List<String> hosts = options.getList(hostsArg.getDest());

    final boolean full = options.getBoolean(fullArg.getDest());

    if (hosts.isEmpty()) {
      hosts = client.listHosts().get();
    }

    final Map<String, ListenableFuture<HostStatus>> futures = Maps.newHashMap();
    for (final String host : hosts) {
      futures.put(host, client.hostStatus(host));
    }

    final Map<String, HostStatus> hostStatuses = Maps.newTreeMap();
    hostStatuses.putAll(Maps.filterValues(allAsMap(futures), not(isNull())));

    if (json) {
      out.println(Json.asPrettyStringUnchecked(hostStatuses));
      return 0;
    }

    final JobStatusTable table = jobStatusTable(out, full);

    for (Map.Entry<String, HostStatus> entry : hostStatuses.entrySet()) {
      final String host = entry.getKey();
      final HostStatus hostStatus = entry.getValue();

      final Set<JobId> jobIds = Sets.newTreeSet();
      jobIds.addAll(hostStatus.getJobs().keySet());
      jobIds.addAll(hostStatus.getStatuses().keySet());
      for (final JobId jobId : jobIds) {
        table.task(jobId, host, hostStatus.getStatuses().get(jobId), hostStatus.getJobs().get(jobId));
      }
    }

    table.print();

    return 0;
  }
}
