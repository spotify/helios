/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.service.Client;
import com.spotify.hermes.message.StatusCode;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobUndeployCommand extends ControlCommand {

  private final Argument jobArg;
  private final Argument hostsArg;

  public JobUndeployCommand(final Subparser parser,
                            final CliConfig cliConfig,
                            final PrintStream out) {
    super(parser, cliConfig, out);

    jobArg = parser.addArgument("job")
        .help("Job id.");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to undeploy the job from.");
  }

  @Override
  int runControl(final Namespace options, final Client client)
      throws ExecutionException, InterruptedException {

    final List<String> hosts = options.getList(hostsArg.getDest());

    final String id = options.getString(jobArg.getDest());

    out.printf("Undeploying %s from %s%n", id, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final StatusCode statusCode = client.undeploy(id, host).get();
      if (statusCode == StatusCode.OK) {
        out.println("done");
      } else {
        out.println("failed: " + statusCode);
        code = -1;
      }
    }

    return code;
  }
}
