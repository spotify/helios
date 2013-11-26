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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobUndeployCommand extends WildcardJobCommand {

  private final Argument hostsArg;

  public JobUndeployCommand(final Subparser parser) {
    super(parser);

    parser.help("undeploy a job from hosts");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to undeploy the job from.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final Client client, final PrintStream out,
                             final boolean json, final JobId jobId)
      throws ExecutionException, InterruptedException, IOException {

    final List<String> hosts = options.getList(hostsArg.getDest());

    out.printf("Undeploying %s from %s%n", jobId, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final JobUndeployResponse response = client.undeploy(jobId, host).get();
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
