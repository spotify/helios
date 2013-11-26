/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.getLast;

abstract class WildcardJobCommand extends ControlCommand {

  private final Argument jobArg;

  public WildcardJobCommand(final Subparser parser) {
    super(parser);

    jobArg = parser.addArgument("job")
        .help("Job id.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException, IOException {

    final String jobIdString = options.getString(jobArg.getDest());
    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (jobs.size() == 0) {
      out.printf("Unknown job: %s%n", jobIdString);
      return 1;
    } else if (jobs.size() > 1) {
      out.printf("Ambiguous job reference: %s%n", jobIdString);
      return 1;
    }

    final JobId jobId = getLast(jobs.keySet());

    return runWithJobId(options, client, out, json, jobId);
  }

  protected abstract int runWithJobId(final Namespace options, final Client client,
                                      final PrintStream out, final boolean json, final JobId jobId)
      throws ExecutionException, InterruptedException, IOException;
}
