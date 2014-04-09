package com.spotify.helios.cli.command;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.protocol.VersionResponse;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class VersionCommand extends ControlCommand {

  public VersionCommand(Subparser parser) {
    super(parser);
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {

    final VersionResponse response = client.version().get();

    if (json) {
      out.println(Json.asPrettyStringUnchecked(response));
    } else {
      out.println(String.format("Client Version: %s%nMaster Version: %s",
                                response.getClientVersion(), response.getMasterVersion()));
    }
    return 0;
  }

}
