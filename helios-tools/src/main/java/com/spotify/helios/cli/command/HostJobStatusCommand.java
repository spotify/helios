package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Joiner.on;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.cli.Utils.truncate;
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

    final Map<JobId, TaskStatus> statuses = status.getStatuses();
    final List<JobId> jobIds = ImmutableList.copyOf(status.getJobs().keySet());

    displayStatuses(out, json, full, jobIds, host, statuses);

    return 0;
  }

  private static void displayStatuses(final PrintStream out, final boolean json, final boolean full,
                                     final List<JobId> jobIds, final String host,
                                     final Map<JobId, TaskStatus> taskStatuses) {
    // TODO (dano): list hosts that have not yet reported a task status
    if (json) {
      out.println(Json.asPrettyStringUnchecked(taskStatuses));
      return;
    }

    // TODO (dano): this explodes the job into one row per host, is that sane/expected?
    final Table table = table(out);
    table.row("JOB ID", "STATE", "CONTAINER ID", "COMMAND",
        "THROTTLED?", "PORTS", "ENVIRONMENT");
    for (final JobId jobId : jobIds) {
      final TaskStatus ts = taskStatuses.get(jobId);
      final String command = on(' ').join(ts.getJob().getCommand());
      final List<String> portMappings = new ArrayList<>();
      for (Map.Entry<String, PortMapping> entry : ts.getPorts().entrySet()) {
        final PortMapping portMapping = entry.getValue();
        portMappings.add(String.format("%s=%d:%d", entry.getKey(),
            portMapping.getInternalPort(),
            portMapping.getExternalPort()));
      }
      final String ports = Joiner.on(" ").join(portMappings);
      final String env = Joiner.on(" ").withKeyValueSeparator("=").join(ts.getEnv());
      final String containerId = Optional.fromNullable(ts.getContainerId()).or("null");
      table.row(full ? jobId : jobId.toShortString(), ts.getState(),
          full ? containerId : truncate(containerId, 7), command, ts.getThrottled(),
              ports, env);
    }
    table.print();
  }
}
