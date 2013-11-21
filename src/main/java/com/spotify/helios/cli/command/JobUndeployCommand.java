/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobUndeployCommand extends ControlCommand {

  private final Argument jobArg;
  private final Argument hostsArg;

  public JobUndeployCommand(final Subparser parser) {
    super(parser);

    parser.help("undeploy a job from hosts");

    jobArg = parser.addArgument("job")
        .help("Job id.");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to undeploy the job from.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {

    final List<String> hosts = options.getList(hostsArg.getDest());

    final JobId id = JobId.fromString(options.getString(jobArg.getDest()));

    out.printf("Undeploying %s from %s%n", id, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final JobUndeployResponse response = client.undeploy(id, host).get();
      if (response.getStatus() == JobUndeployResponse.Status.OK) {
        out.println("done");
      } else {
        out.println("failed: " + response);
        code = -1;
      }
    }

    return code;
  }
}
