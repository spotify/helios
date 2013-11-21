/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobIdParseException;
import com.spotify.helios.common.protocol.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.append;

public class JobStatusCommand extends ControlCommand {

  private final Argument jobsArg;

  public JobStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show status for a job");

    jobsArg = parser.addArgument("job")
        .action(append())
        .setDefault(Lists.newArrayList())
        .help("Job id");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> jobIdStrings = options.getList(jobsArg.getDest());
    final List<JobId> jobIds = Lists.newArrayList();
    for (final String jobIdString : jobIdStrings) {
      try {
        jobIds.add(JobId.parse(jobIdString));
      } catch (JobIdParseException e) {
        if (!json) {
          out.println("Invalid job id: " + jobIdString);
        }
        return 0;
      }
    }

    final Map<JobId, JobStatus> statuses = Maps.newHashMap();
    for (final JobId jobId : jobIds) {
      statuses.put(jobId, client.jobStatus(jobId).get());
    }

    if (json) {
      out.print(Json.asPrettyStringUnchecked(statuses));
    } else {
      // TODO (dano): pretty output
      out.println(statuses);
    }

    return 0;
  }
}
