package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostJobStatusCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument fullArg;

  public HostJobStatusCommand(Subparser parser) {
    super(parser);

    parser.help("show status for all jobs on a host");

    hostArg = parser.addArgument("host")
        .help("The hosts to list jobs for.");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job and container id's.");

  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, boolean json)
      throws ExecutionException, InterruptedException, IOException {
    final String host = options.getString(hostArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());
    final HostStatus status = client.hostStatus(host).get();

    final Map<JobId, Deployment> jobs = status.getJobs();
    final Map<JobId, JobStatus> statuses = Maps.newHashMap();

    for (Entry<JobId, Deployment> entry : jobs.entrySet()) {
      statuses.put(entry.getKey(), client.jobStatus(entry.getKey()).get());
    }

    final List<JobId> jobIds = ImmutableList.copyOf(status.getJobs().keySet());
    JobStatusCommand.displayStatuses(out, json, full, jobIds, statuses);

    return 0;
  }
}
