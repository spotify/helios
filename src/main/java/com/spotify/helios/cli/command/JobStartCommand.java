/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.SetGoalResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobStartCommand extends ControlCommand {

  private final Argument hostsArg;
  private final Argument jobArg;

  public JobStartCommand(Subparser parser) {
    super(parser);

    parser.help("start a job on hosts");

    jobArg = parser.addArgument("job")
        .help("Job to start.");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to start the job on.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> hosts = options.getList(hostsArg.getDest());
    final JobId jobId = JobId.fromString(options.getString(jobArg.getDest()));

    final Deployment deployment = new Deployment.Builder()
        .setGoal(Goal.START)
        .setJobId(jobId)
        .build();

    out.printf("Starting %s on %s%n", jobId, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final SetGoalResponse result = client.setGoal(deployment, host).get();
      if (result.getStatus() == SetGoalResponse.Status.OK) {
        out.printf("done%n");
      } else {
        out.printf("failed: %s%n", result);
        code = 1;
      }
    }

    return code;
  }
}