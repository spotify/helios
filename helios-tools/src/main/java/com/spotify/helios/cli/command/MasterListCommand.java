package com.spotify.helios.cli.command;

import com.spotify.helios.client.HeliosClient;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MasterListCommand extends ControlCommand {

  public MasterListCommand(Subparser parser) {
    super(parser);
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> masters = client.listMasters().get();
    for (final String master : masters) {
      out.println(master);
    }
    return 0;
  }
}
