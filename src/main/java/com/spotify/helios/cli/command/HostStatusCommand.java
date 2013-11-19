/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.lang.Math.max;
import static java.lang.String.format;
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

    // TODO (dano): provide more detailed info when user provides more -v flags
    // TODO (dano): -v should then not control the logging level, instead a --debug flag could do that

    // TODO (dano): this flat table of hosts should maybe go into the host list and job status commands

    final Table table = new Table(out);
    table.row("host", "status", "jobs", "running", "cpus", "mem", "load avg", "mem usage", "os",
              "version");
    for (final String host : hosts) {
      final AgentStatus s = client.agentStatus(host).get();

      if (s != null) {
        final Set<JobStatus> runningDeployedJobs = Sets.newHashSet();
        for (final String name : s.getJobs().keySet()) {
          final JobStatus jobStatus = s.getStatuses().get(name);
          if (jobStatus.getState() == JobStatus.State.RUNNING) {
            runningDeployedJobs.add(jobStatus);
          }
        }

        final long free = s.getHostInfo().getMemoryFreeBytes();
        final long total = s.getHostInfo().getMemoryTotalBytes();
        final float memUsage = (float) (total - free) / total;
        table.row(host, s.getStatus(), s.getJobs().size(), runningDeployedJobs.size(),
                  s.getHostInfo().getCpus(),
                  s.getHostInfo().getMemoryTotalBytes() / (1024 * 1024 * 1024) + " gb",
                  format("%.2f", s.getHostInfo().getLoadAvg()),
                  format("%.2f", memUsage),
                  s.getHostInfo().getOsName(),
                  s.getHostInfo().getOsVersion());
      } else {
        table.row(host, "UNKNOWN");
      }
    }

    table.print();

    return 0;
  }

  private class Table {

    private final PrintStream out;
    private int[] columns = new int[0];
    private final List<Object[]> rows = Lists.newArrayList();

    private Table(final PrintStream out) {
      this.out = out;
    }

    public void row(final Object... row) {
      columns = Ints.ensureCapacity(columns, row.length, row.length);
      for (int i = 0; i < row.length; i++) {
        row[i] = row[i].toString();
        columns[i] = max(columns[i], row[i].toString().length());
      }
      rows.add(row);
    }

    public void print() {
      for (final Object[] row : rows) {
        for (int i = 0; i < row.length; i++) {
          final String cell = row[i].toString();
          out.print(cell);
          out.print("    ");
          final int padding = columns[i] - cell.length();
          for (int j = 0; j < padding; j++) {
            out.print(' ');
          }
        }
        out.println();
      }
    }
  }
}
