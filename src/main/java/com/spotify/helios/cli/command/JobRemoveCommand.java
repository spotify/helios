package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class JobRemoveCommand extends WildcardJobCommand {

  private final Argument forceArg;

  public JobRemoveCommand(Subparser parser) {
    super(parser);

    parser.help("remove a job");

    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Force removal.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final Client client, final PrintStream out,
                             final boolean json, final JobId jobId)
      throws IOException, ExecutionException, InterruptedException {
    final boolean force = options.getBoolean(forceArg.getDest());

    if (!force) {
      out.printf("This will remove the job %s%n", jobId);
      out.printf("Do you want to continue? [Y/n]%n");

      // TODO (dano): pass in stdin instead using System.in
      final int c = System.in.read();

      if (c != 'Y' && c != 'y') {
        return 1;
      }
    }

    out.printf("Removing job %s%n", jobId);

    int code = 0;

    final JobDeleteResponse response = client.deleteJob(jobId).get();
    out.printf("%s: ", jobId);
    if (response.getStatus() == JobDeleteResponse.Status.OK) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", response);
      code = 1;
    }
    return code;
  }
}
