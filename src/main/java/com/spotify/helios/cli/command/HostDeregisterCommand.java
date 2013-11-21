package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.protocol.AgentDeleteResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class HostDeregisterCommand extends ControlCommand {

  private Argument agentArg;
  private Argument confirmArg;

  public HostDeregisterCommand(Subparser parser) {
    super(parser);

    parser.help("deregister a host");

    agentArg = parser.addArgument("host")
        .help("Host name to deregister.");

    // TODO(drewc): perhaps require the enter in today's date or something?
    confirmArg = parser.addArgument("sure")
        .help("Are you really sure?  Set arg to yes.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out)
      throws ExecutionException, InterruptedException {
    String host = options.getString(agentArg.getDest());

    if (!"yes".equals(options.getString(confirmArg.getDest()))) {
      out.printf("Will not delete a host unconfirmed.  Add yes to your command line.");
      return 1;
    }

    out.printf("Removing agent %s%n", host);

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
