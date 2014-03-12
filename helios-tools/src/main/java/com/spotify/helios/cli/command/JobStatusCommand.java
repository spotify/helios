/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Optional.fromNullable;
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

    final Collection<JobId> jobIds = Sets.newTreeSet();
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




    // TODO (dano): list hosts that have not yet reported a task status
    if (json) {
      out.println(Json.asPrettyStringUnchecked(statuses));
      return 0;
    }

    final int maxContainerId = full ? Integer.MAX_VALUE : 7;

    // TODO (dano): this explodes the job into one row per host, is that sane/expected?
    final Table table = table(out);
    table.row("JOB ID", "HOST", "GOAL", "STATE", "CONTAINER ID", "PORTS");

    for (final JobId jobId : jobIds) {
      final JobStatus jobStatus = statuses.get(jobId);

      // Merge hosts without any status into the set of hosts with a reported task status
      final Map<String, TaskStatus> taskStatuses = Maps.newTreeMap();
      taskStatuses.putAll(jobStatus.getTaskStatuses());
      for (final String host : jobStatus.getDeployedHosts()) {
        if (!taskStatuses.containsKey(host)) {
          taskStatuses.put(host, null);
        }
      }

      for (final String host : taskStatuses.keySet()) {
        final Map<String, Deployment> deployments = jobStatus.getDeployments();
        final Deployment deployment = (deployments == null) ? null : deployments.get(host);
        final String goal = (deployment == null) ? "" : deployment.getGoal().toString();
        final TaskStatus ts = taskStatuses.get(host);
        final String jobIdString = full ? jobId.toString() : jobId.toShortString();
        if (ts == null) {
          table.row(jobIdString, host, goal, "", "", "");
        } else {
          final List<String> portMappings = new ArrayList<>();
          for (Map.Entry<String, PortMapping> entry : ts.getPorts().entrySet()) {
            final PortMapping portMapping = entry.getValue();
            portMappings.add(String.format("%s=%d:%d", entry.getKey(),
                                           portMapping.getInternalPort(),
                                           portMapping.getExternalPort()));
          }
          String state = ts.getState().toString();
          if (ts.getThrottled() != ThrottleState.NO) {
            state += " (" + ts.getThrottled() + ")";
          }
          final String ports = Joiner.on(" ").join(portMappings);
          final String cid = truncate(fromNullable(ts.getContainerId()).or(""), maxContainerId);
          table.row(jobIdString, host, goal, state, cid,  ports);
        }
      }
    }

    table.print();

    return 0;
  }
}
