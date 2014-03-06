/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
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
import static com.spotify.helios.cli.Output.humanDuration;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.String.format;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;

  public HostListCommand(final Subparser parser) {
    super(parser);

    parser.help("list hosts");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .help("Host pattern to match hosts on");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print host names");

  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final String pattern = options.getString(patternArg.getDest());
    final List<String> hosts = ImmutableList.copyOf(Iterables.filter(client.listHosts().get(),
      new Predicate<String>() {
        @Override
        public boolean apply(String host) {
          if (pattern == null) {
            return true;
          }
          return host.contains(pattern);
        }
    }));

    final List<String> sortedHosts = natural().sortedCopy(hosts);


    final boolean quiet = options.getBoolean(quietArg.getDest());

    if (quiet) {
      for (final String host : sortedHosts) {
        out.println(host);
      }
    } else {
      final Table table = table(out);
      table.row("HOST", "STATUS", "DEPLOYED", "RUNNING",
                "CPUS", "MEM", "LOAD AVG", "MEM USAGE", "OS", "VERSION");

      final Map<String, ListenableFuture<HostStatus>> statuses = Maps.newTreeMap();
      for (final String host : hosts) {
        statuses.put(host, client.hostStatus(host));
      }

      for (final Map.Entry<String, ListenableFuture<HostStatus>> e : statuses.entrySet()) {

        final String host = e.getKey();
        final HostStatus s = e.getValue().get();

        if (s == null) {
          continue;
        }

        final Set<TaskStatus> runningDeployedJobs = Sets.newHashSet();
        for (final JobId jobId : s.getJobs().keySet()) {
          final TaskStatus taskStatus = s.getStatuses().get(jobId);
          if (taskStatus == null) {
            continue;
          }
          if (taskStatus.getState() == TaskStatus.State.RUNNING) {
            runningDeployedJobs.add(taskStatus);
          }
        }

        final HostInfo hi = s.getHostInfo();
        final String memUsage;
        final String cpus;
        final String mem;
        final String loadAvg;
        final String osName;
        final String osVersion;
        if (hi != null) {
          final long free = hi.getMemoryFreeBytes();
          final long total = hi.getMemoryTotalBytes();
          memUsage = format("%.2f", (float) (total - free) / total);
          cpus = String.valueOf(hi.getCpus());
          mem = hi.getMemoryTotalBytes() / (1024 * 1024 * 1024) + " gb";
          loadAvg = format("%.2f", hi.getLoadAvg());
          osName = hi.getOsName();
          osVersion = hi.getOsVersion();
        } else {
          memUsage = cpus = mem = loadAvg = osName = osVersion = "";
        }

        final String status;
        if (s.getStatus() == UP) {
          final String uptime = s.getAgentInfo() == null ? "" :
                                humanDuration(System.currentTimeMillis() -
                                              s.getAgentInfo().getStartTime());
          status = "Up " + uptime;
        } else {
          status = "Down";
        }

        table.row(host, status, s.getJobs().size(), runningDeployedJobs.size(),
                  cpus, mem, loadAvg, memUsage, osName, osVersion);
      }

      table.print();
    }

    return 0;
  }
}
