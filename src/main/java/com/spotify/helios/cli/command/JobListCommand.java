/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Joiner.on;
import static com.spotify.helios.cli.Output.table;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;

  public JobListCommand(final Subparser parser) {
    super(parser);

    parser.help("list jobs");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .help("Job reference to filter on");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final boolean quiet = options.getBoolean(quietArg.getDest());
    final String pattern = options.getString(patternArg.getDest());

    final Map<JobId, Job> jobs;
    if (pattern == null) {
      jobs = client.jobs().get();
    } else {
      jobs = client.jobs(pattern).get();
    }

    final SortedSet<JobId> sortedJobIds = Sets.newTreeSet(jobs.keySet());

    if (json) {
      if (quiet) {
        out.println(Json.asPrettyStringUnchecked(sortedJobIds));
      } else {
        out.println(Json.asPrettyStringUnchecked(jobs));
      }
    } else {
      if (quiet) {
        for (final JobId jobId : sortedJobIds) {
          out.println(jobId);
        }
      } else {
        final Table table = table(out);
        table.row("JOB ID", "NAME", "VERSION", "HOSTS", "COMMAND", "ENVIRONMENT");

        final Map<JobId, ListenableFuture<JobStatus>> statuses = Maps.newTreeMap();
        for (final JobId jobId : sortedJobIds) {
          statuses.put(jobId, client.jobStatus(jobId));
        }

        for (final Map.Entry<JobId, ListenableFuture<JobStatus>> e : statuses.entrySet()) {
          final JobId jobId = e.getKey();
          final JobStatus status = e.getValue().get();
          final Job job = jobs.get(jobId);
          final String command = on(' ').join(job.getCommand());
          final String env = Joiner.on(" ").withKeyValueSeparator("=").join(job.getEnv());
          table.row(jobId, jobId.getName(), jobId.getVersion(), status.getDeployedHosts().size(),
                    command, env);
        }
        table.print();
      }
    }

    return 0;
  }
}

