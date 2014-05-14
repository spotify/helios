/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.client.HeliosClient;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class HostRegisterCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument idArg;

  public HostRegisterCommand(final Subparser parser) {
    super(parser);

    parser.help("register a host");

    hostArg = parser.addArgument("host");
    idArg = parser.addArgument("id");

  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final String host = options.getString(hostArg.getDest());
    final String id = options.getString(idArg.getDest());

    out.printf("Registering host %s with id %s%n", host, id);

    int code = 0;
    out.printf("%s: ", host);
    final int result = client.registerHost(host, id).get();
    if (result == 200) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", result);
      code = 1;
    }

    return code;
  }
}
