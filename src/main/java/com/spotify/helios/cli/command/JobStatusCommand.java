/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.Client;
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

public class JobStatusCommand extends ControlCommand {

  private final Argument jobsArg;

  public JobStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show status for a job");

    jobsArg = parser.addArgument("job")
        .nargs("+")
        .help("Job reference");
  }

  @Override
  int run(Namespace options, Client client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final List<String> jobIdStrings = options.getList(jobsArg.getDest());
    final List<JobId> jobIds = Lists.newArrayList();
    for (final String jobIdString : jobIdStrings) {
      final Map<JobId, Job> jobs = client.jobs(jobIdString).get();
      // TODO (dano): complain if there were no matching jobs?
      jobIds.addAll(jobs.keySet());
    }

    // TODO (dano): it would sure be nice to be able to report container/task uptime

    final Map<JobId, JobStatus> statuses = Maps.newHashMap();
    for (final JobId jobId : jobIds) {
      statuses.put(jobId, client.jobStatus(jobId).get());
    }

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
          String containerId = ts.getContainerId();
          table.row(jobId, host, ts.getState(), containerId == null ? "null" : containerId,
              command, ts.getThrottled(), ports, env);
        }
      }
      table.print();
    }

    return 0;
  }

}
