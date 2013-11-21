package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class JobRemoveCommand extends ControlCommand {

  private final Argument jobIdArg;
  private final Argument confirmArg;

  public JobRemoveCommand(Subparser parser) {
    super(parser);

    parser.help("remove a job");

    jobIdArg = parser.addArgument("jobid")
        .help("The id of the job to remove.");

    // TODO(drewc): perhaps require the enter in today's date or something?
    confirmArg = parser.addArgument("sure")
        .help("Are you really sure?  Set arg to yes.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final JobId jobId = JobId.fromString(options.getString(jobIdArg.getDest()));

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
