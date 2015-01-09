/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.cli.command;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.JobStatusTable;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.containsPattern;
import static com.spotify.helios.cli.Output.formatHostname;
import static com.spotify.helios.cli.Output.jobStatusTable;
import static com.spotify.helios.cli.Utils.allAsMap;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobStatusCommand extends ControlCommand {

  private final Argument jobArg;
  private final Argument hostArg;
  private final Argument fullArg;

  public JobStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show job or host status");

    jobArg = parser.addArgument("-j", "--job")
        .help("Job filter");

    hostArg = parser.addArgument("--host")
        .setDefault("")
        .help("Host pattern");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full hostnames, job and container id's.");
  }

  interface HostStatusDisplayer {
    void matchedStatus(final JobStatus jobStatus, final Iterable<String> matchingHosts,
                       final Map<String, TaskStatus> taskStatuses);
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final String jobIdString = options.getString(jobArg.getDest());
    final String hostPattern = options.getString(hostArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());

    final Map<JobId, Job> jobs;
    if (Strings.isNullOrEmpty(jobIdString)) {
      jobs = client.jobs().get();
    } else {
      jobs = client.jobs(jobIdString).get();
    }

    if (jobs == null) {
      out.printf("The specified Helios master either returned an error or job id matcher "
                 + "\"%s\" matched no jobs%n", jobIdString);
      return 1;
    }

    final Set<JobId> jobIds = jobs.keySet();

    if (!Strings.isNullOrEmpty(jobIdString) && jobIds.isEmpty()) {
      out.printf("job id matcher \"%s\" matched no jobs%n", jobIdString);
      return 1;
    }

    // TODO (dano): it would sure be nice to be able to report container/task uptime
    final Map<JobId, ListenableFuture<JobStatus>> futures = 
        JobStatusFetcher.getJobsStatuses(client, jobIds);
    final Map<JobId, JobStatus> statuses = Maps.newTreeMap();
    statuses.putAll(allAsMap(futures));

    if (json) {
      showJsonStatuses(out, hostPattern, jobIds, statuses);
      return 0;
    }

    final JobStatusTable table = jobStatusTable(out, full);

    final boolean noHostMatchedEver = showStatusesForHosts(hostPattern, jobIds, statuses,
      new HostStatusDisplayer() {
        @Override
        public void matchedStatus(JobStatus jobStatus, Iterable<String> matchingHosts,
                                  Map<String, TaskStatus> taskStatuses) {
          displayTask(full, table, jobStatus.getJob().getId(), jobStatus, taskStatuses,
              matchingHosts);
        }
    });

    if (noHostMatchedEver) {
      String domainsSwitchString = "";

      final List<String> domains = options.get("domains");
      final List<String> sites = options.get("sites");
      if (domains.size() > 0) {
        domainsSwitchString = "-d " + Joiner.on(",").join(domains);
      } else if (sites.size() > 0) {
        domainsSwitchString = "-d " + Joiner.on(",").join(sites);
      }
      out.printf("There are no jobs deployed to hosts with the pattern '%s'%n" +
                 "Run 'helios %s hosts %s' to check it exists and is up.%n",
                 hostPattern, domainsSwitchString, hostPattern);
      return 1;
    }

    table.print();

    return 0;
  }

  private void showJsonStatuses(PrintStream out, final String hostPattern, final Set<JobId> jobIds,
      final Map<JobId, JobStatus> statuses) {
    if (Strings.isNullOrEmpty(hostPattern)) {
      out.println(Json.asPrettyStringUnchecked(statuses));
      return;
    }

    final Map<JobId, JobStatus> returnStatuses = Maps.newTreeMap();
    showStatusesForHosts(hostPattern, jobIds, statuses, new HostStatusDisplayer() {
      @Override
      public void matchedStatus(JobStatus jobStatus, Iterable<String> matchingHosts,
                                Map<String, TaskStatus> taskStatuses) {
        for (final String host : matchingHosts) {
          final Map<String, Deployment> deployments = jobStatus.getDeployments();
          final Deployment deployment = (deployments == null) ? null : deployments.get(host);
          if (deployment != null) {
            returnStatuses.put(jobStatus.getJob().getId(),
                filterJobStatus(jobStatus, matchingHosts));
          }
        }
      }

    });
    out.println(Json.asPrettyStringUnchecked(returnStatuses));
  }

  private JobStatus filterJobStatus(final JobStatus jobStatus,
      final Iterable<String> matchingHosts) {
    final Map<String, TaskStatus> taskStatuses = Maps.newHashMap(jobStatus.getTaskStatuses());
    final Set<String> matchingHostSet = Sets.newHashSet(matchingHosts);

    for (final String key : Sets.newHashSet(taskStatuses.keySet())) {
      if (!matchingHostSet.contains(key)) {
        taskStatuses.remove(key);
      }
    }

    final Map<String, Deployment> deployments = Maps.newHashMap(jobStatus.getDeployments());
    for (final String key : Sets.newHashSet(deployments.keySet())) {
      if (!matchingHostSet.contains(key)) {
        deployments.remove(key);
      }
    }
    return JobStatus.newBuilder()
        .setJob(jobStatus.getJob())
        .setDeployments(deployments)
        .setTaskStatuses(taskStatuses)
        .build();
  }

  private boolean showStatusesForHosts(final String hostPattern, final Set<JobId> jobIds,
      final Map<JobId, JobStatus> statuses, final HostStatusDisplayer statusDisplayer) {
    boolean noHostMatchedEver = true;

    for (final JobId jobId : Ordering.natural().sortedCopy(jobIds)) {
      final JobStatus jobStatus = statuses.get(jobId);

      // jobStatus will be null if the job was deleted after we first got the list of job IDs
      if (jobStatus == null) {
        continue;
      }

      final Map<String, TaskStatus> taskStatuses = Maps.newTreeMap();
      taskStatuses.putAll(jobStatus.getTaskStatuses());

      // Add keys for jobs that were deployed to a host,
      // but for which we didn't get a reported task status.
      // This will help us see hosts where jobs aren't running correctly.
      for (final String host : jobStatus.getDeployments().keySet()) {
        if (!taskStatuses.containsKey(host)) {
          taskStatuses.put(host, null);
        }
      }

      final FluentIterable<String> matchingHosts = FluentIterable
          .from(taskStatuses.keySet())
          .filter(containsPattern(hostPattern));

      if (Strings.isNullOrEmpty(hostPattern) ||
          !Strings.isNullOrEmpty(hostPattern) && !matchingHosts.isEmpty()) {
        noHostMatchedEver = false;
      }

      statusDisplayer.matchedStatus(jobStatus, matchingHosts, taskStatuses);

    }
    return noHostMatchedEver;
  }

  private void displayTask(final boolean full, final JobStatusTable table, final JobId jobId,
      final JobStatus jobStatus, final Map<String, TaskStatus> taskStatuses,
      final Iterable<String> matchingHosts) {
    for (final String host : matchingHosts) {
      final Map<String, Deployment> deployments = jobStatus.getDeployments();
      final TaskStatus ts = taskStatuses.get(host);
      final Deployment deployment = (deployments == null) ? null : deployments.get(host);
      table.task(jobId, formatHostname(full, host), ts, deployment);
    }
  }
}
