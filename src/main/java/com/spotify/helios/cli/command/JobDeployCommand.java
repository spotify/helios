/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeployResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobDeployCommand extends WildcardJobCommand {

  private final Argument hostsArg;
  private final Argument noStartArg;

  public JobDeployCommand(final Subparser parser) {
    super(parser);

    parser.help("deploy a job to hosts");

    noStartArg = parser.addArgument("--no-start")
        .action(storeTrue())
        .help("Deploy job without starting it.");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to deploy the job on.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final Client client, final PrintStream out,
                             final boolean json, final JobId jobId)
      throws ExecutionException, InterruptedException {
    final List<String> hosts = options.getList(hostsArg.getDest());

    final Deployment job = Deployment.of(jobId,
                                         options.getBoolean(noStartArg.getDest()) ? STOP : START);

    out.printf("Deploying %s on %s%n", job, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final JobDeployResponse result = client.deploy(job, host).get();
      if (result.getStatus() == JobDeployResponse.Status.OK) {
        out.printf("done%n");
      } else {
        out.printf("failed: %s%n", result);
        code = 1;
      }
    }

    return code;
  }
}
