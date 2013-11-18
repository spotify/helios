/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.hermes.message.StatusCode;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HostRegisterCommand extends ControlCommand {

  private final Argument hostsArg;

  public HostRegisterCommand(final Subparser parser) {
    super(parser);

    hostsArg = parser.addArgument("hosts").nargs("+");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out)
      throws ExecutionException, InterruptedException {
    final List<String> hosts = options.getList(hostsArg.getDest());

    out.printf("Registering hosts: %s%n", hosts);

    int code = 0;
    for (final String host : hosts) {
      out.printf("%s: ", host);
      final StatusCode result = client.registerAgent(host).get();
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
