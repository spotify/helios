package com.spotify.helios.cli.command;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.getLast;

public class JobRemoveCommand extends ControlCommand {

  private final Argument jobIdArg;
  private final Argument forceArg;

  public JobRemoveCommand(Subparser parser) {
    super(parser);

    parser.help("remove a job");

    jobIdArg = parser.addArgument("jobid")
        .help("The id of the job to remove.");

    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Force removal.");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException, IOException {
    final String jobIdString = options.getString(jobIdArg.getDest());
    final boolean force = options.getBoolean(forceArg.getDest());

    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (jobs.size() == 0) {
      out.printf("Unknown job: %s%n", jobIdString);
      return 1;
    } else if (jobs.size() > 1) {
      out.printf("Ambiguous job reference: %s%n", jobIdString);
      return 1;
    }

    final JobId jobId = getLast(jobs.keySet());

    if (!force) {
      out.printf("This will remove the job %s%n", jobId);
      out.printf("Do you want to continue? [Y/n]%n");

      final int c = System.in.read();

      if (c != 'Y') {
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
