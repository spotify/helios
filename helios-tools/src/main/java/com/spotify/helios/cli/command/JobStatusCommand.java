/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Joiner.on;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.cli.Utils.allAsMap;
import static com.spotify.helios.cli.Utils.truncate;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobStatusCommand extends ControlCommand {

  private final Argument jobsArg;
  private final Argument fullArg;

  public JobStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show status for a job");

    jobsArg = parser.addArgument("job")
        .nargs("+")
        .help("Job reference");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job and container id's.");
  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> jobIdStrings = options.getList(jobsArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());
    final List<ListenableFuture<Map<JobId, Job>>> jobIdFutures = Lists.newArrayList();
    for (final String jobIdString : jobIdStrings) {
      // TODO (dano): complain if there were no matching jobs?
      jobIdFutures.add(client.jobs(jobIdString));
    }

    final List<JobId> jobIds = Lists.newArrayList();
    for (ListenableFuture<Map<JobId, Job>> future : jobIdFutures) {
      jobIds.addAll(future.get().keySet());
    }

    // TODO (dano): it would sure be nice to be able to report container/task uptime

    final Map<JobId, ListenableFuture<JobStatus>> futures = Maps.newTreeMap();
    for (final JobId jobId : jobIds) {
      futures.put(jobId, client.jobStatus(jobId));
    }
    final Map<JobId, JobStatus> statuses = Maps.newTreeMap();
    statuses.putAll(allAsMap(futures));

    if (json) {
      out.println(Json.asPrettyStringUnchecked(statuses));
    } else {
      // TODO (dano): this explodes the job into one row per agent, is that sane/expected?
      final Table table = table(out);
      table.row("JOB ID", "HOST", "STATE", "CONTAINER ID", "COMMAND",
                "THROTTLED?", "PORTS", "ENVIRONMENT");
      for (final JobId jobId : jobIds) {
        final JobStatus jobStatus = statuses.get(jobId);
        final Map<String, TaskStatus> taskStatuses = jobStatus.getTaskStatuses();
        for (final String host : taskStatuses.keySet()) {
          final TaskStatus ts = taskStatuses.get(host);
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
          table.row(full ? jobId : jobId.toShortString(), host, ts.getState(),
                    full ? containerId : truncate(containerId, 7), command, ts.getThrottled(),
                    ports, env);
        }
      }
      table.print();
    }

    return 0;
  }
}
