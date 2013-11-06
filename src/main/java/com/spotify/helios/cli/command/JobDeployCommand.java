/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;
import com.spotify.helios.service.descriptors.AgentJob;
import com.spotify.hermes.message.StatusCode;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.service.descriptors.JobGoal.START;
import static com.spotify.helios.service.descriptors.JobGoal.STOP;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobDeployCommand extends ControlCommand {

  private final Argument jobArg;
  private final Argument hostsArg;
  private final Argument noStartArg;

  public JobDeployCommand(
      final Subparser parser,
      final CliConfig cliConfig,
      final PrintStream out) {
    super(parser, cliConfig, out);

    jobArg = parser.addArgument("job")
        .help("Job id.");

    noStartArg = parser.addArgument("--no-start")
        .action(storeTrue())
        .help("Deploy job without starting it.");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to deploy the job on.");
  }

  @Override
  int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {
    final List<String> hosts = options.getList(hostsArg.getDest());
    final AgentJob job = AgentJob.of(options.getString(jobArg.getDest()),
                                     options.getBoolean(noStartArg.getDest()) ? STOP : START);

    out.printf("Deploying %s on %s%n", job, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final StatusCode result = client.deploy(job, host).get();
      if (result == StatusCode.OK) {
        out.printf("done%n");
      } else {
        out.printf("failed: %s%n", result);
        code = 1;
      }
    }

    return code;
  }
}
