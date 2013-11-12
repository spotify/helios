package com.spotify.helios.cli.command;

import com.spotify.helios.cli.CliConfig;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.protocol.JobDeleteResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class JobRemoveCommand extends ControlCommand {
  private final Argument jobIdArg;
  private final Argument confirmArg;

  public JobRemoveCommand(Subparser parser, CliConfig cliConfig, PrintStream out) {
    super(parser, cliConfig, out);

    jobIdArg = parser.addArgument("jobid")
        .help("The id of the job to remove.");

    // TODO(drewc): perhaps require the enter in today's date or something?
    confirmArg = parser.addArgument("sure")
        .help("Are you really sure?  Set arg to yes.");
  }

  @Override
  int runControl(Namespace options, Client client) throws ExecutionException, InterruptedException {
    String jobId = options.getString(jobIdArg.getDest());

    if (!"yes".equals(options.getString(confirmArg.getDest()))) {
      out.printf("Will not delete a job unconfirmed.  Add yes to your command line.");
      return 1;
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
