package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.protocol.AgentDeleteResponse;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class HostDeregisterCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument forceArg;

  public HostDeregisterCommand(Subparser parser) {
    super(parser);

    parser.help("deregister a host");

    hostArg = parser.addArgument("host")
        .help("Host name to deregister.");

    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Force deregistration.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException, IOException {
    final String host = options.getString(hostArg.getDest());
    final boolean force = options.getBoolean(forceArg.getDest());

    if (!force) {
      out.printf("This will deregister the host %s%n", host);
      out.printf("Do you want to continue? [Y/n]%n");

      // TODO (dano): pass in stdin instead using System.in
      final int c = System.in.read();

      if (c != 'Y' && c != 'y') {
        return 1;
      }
    }

    out.printf("Deregistering host %s%n", host);

    int code = 0;

    final AgentDeleteResponse response = client.deleteAgent(host).get();
    out.printf("%s: ", host);
    if (response.getStatus() == AgentDeleteResponse.Status.OK) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", response);
      code = 1;
    }
    return code;
  }
}
