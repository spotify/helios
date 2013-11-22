/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Sets;

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

  public JobListCommand(final Subparser parser) {
    super(parser);

    parser.help("list all jobs");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Map<JobId, Job> jobs = client.jobs().get();

    SortedSet<JobId> sortedJobIds = Sets.newTreeSet(jobs.keySet());

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
        table.row("JOB ID", "NAME", "VERSION", "HOSTS", "COMMAND");
        for (final JobId jobId : sortedJobIds) {
          final Job job = jobs.get(jobId);
          final JobStatus status = client.jobStatus(jobId).get();
          final String command = on(' ').join(job.getCommand());
          table.row(jobId, jobId.getName(), jobId.getVersion(), status.getDeployedHosts().size(),
                    command);
        }
        table.print();
      }
    }

    return 0;
  }
}

