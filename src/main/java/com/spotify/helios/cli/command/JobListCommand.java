/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Sets;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Job;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobListCommand extends ControlCommand {

  private final Argument quietArg;

  public JobListCommand(final Subparser parser) {
    super(parser);

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out)
      throws ExecutionException, InterruptedException {
    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Map<String, Job> jobs = client.jobs().get();

    SortedSet<String> sortedJobIds = Sets.newTreeSet(jobs.keySet());

    for (final String jobId : sortedJobIds) {
      if (quiet) {
        out.println(jobId);
      } else {
        final Job job = jobs.get(jobId);
        out.printf("%s: %s %s%n", job.getId(), job.getImage(), job.getCommand());
      }
    }

    return 0;
  }
}

