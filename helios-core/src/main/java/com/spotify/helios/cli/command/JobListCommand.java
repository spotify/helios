/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Sets;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;
import com.spotify.helios.service.descriptors.JobDescriptor;

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

  public JobListCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");
  }

  @Override
  int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {
    final boolean quiet = options.getBoolean(quietArg.getDest());

    final Map<String, JobDescriptor> jobs = client.jobs().get();

    SortedSet<String> sortedJobIds = Sets.newTreeSet(jobs.keySet());

    for (final String jobId : sortedJobIds) {
      if (quiet) {
        out.println(jobId);
      } else {
        final JobDescriptor d = jobs.get(jobId);
        out.printf("%s: %s %s %s %s %s%n",
                   jobId, d.getName(), d.getVersion(), d.getHash(), d.getImage(), d.getCommand());
      }
    }

    return 0;
  }
}

