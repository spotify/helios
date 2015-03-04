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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Target;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.cli.Utils.allAsMap;
import static com.spotify.helios.cli.command.JobStatusFetcher.getJobsStatuses;
import static java.lang.String.format;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobWatchCommand extends MultiTargetControlCommand {

  private final Argument prefixesArg;
  private final Argument jobsArg;
  private final Argument intervalArg;
  private final Argument exactArg;

  public JobWatchCommand(Subparser parser) {
    super(parser);
    parser.help("watch jobs");

    jobsArg = parser.addArgument("job")
        .help("Job reference");

    intervalArg = parser.addArgument("--interval")
        .type(Integer.class)
        .setDefault(1)
        .help("polling interval, default 1 second");

    prefixesArg = parser.addArgument("hosts")
        .nargs("*")
        .help("The hostname prefixes to watch the job on.");

    exactArg = parser.addArgument("--exact")
        .action(storeTrue())
        .help("Show status of job for every host in hosts");

  }

  @Override
  int run(final Namespace options, final List<TargetAndClient> clients,
          final PrintStream out, final boolean json, final BufferedReader stdin)
              throws ExecutionException, InterruptedException, IOException {
    final boolean exact = options.getBoolean(exactArg.getDest());
    final List<String> prefixes = options.getList(prefixesArg.getDest());
    final String jobIdString = options.getString(jobsArg.getDest());
    final List<ListenableFuture<Map<JobId, Job>>> jobIdFutures = Lists.newArrayList();
    for (final TargetAndClient cc : clients) {
      jobIdFutures.add(cc.getClient().jobs(jobIdString));
    }

    final Set<JobId> jobIds = Sets.newHashSet();
    for (final ListenableFuture<Map<JobId, Job>> future : jobIdFutures) {
      jobIds.addAll(future.get().keySet());
    }

    watchJobsOnHosts(out, exact, prefixes, jobIds, options.getInt(intervalArg.getDest()),
        clients);
    return 0;
  }



  public static void watchJobsOnHosts(final PrintStream out, final boolean exact,
                                      final List<String> resolvedHosts, final List<JobId> jobIds,
                                      final Integer interval, final HeliosClient client)
                                          throws InterruptedException, ExecutionException {
    watchJobsOnHosts(out, exact, resolvedHosts, Sets.newHashSet(jobIds), interval,
        ImmutableList.of(new TargetAndClient(client)));
  }

  static void watchJobsOnHosts(final PrintStream out, final boolean exact,
                               final List<String> prefixes, final Set<JobId> jobIds,
                               final int interval, final List<TargetAndClient> clients)
      throws InterruptedException, ExecutionException {
    out.println("Control-C to stop");
    out.println("JOB                  HOST                           STATE    THROTTLED?");
    final DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
    while (true) {

      final Instant now = new Instant();
      out.printf("-------------------- ------------------------------ -------- "
          + "---------- [%s UTC]%n", now.toString(formatter));
      for (TargetAndClient cc : clients) {
        final Optional<Target> target = cc.getTarget();
        if (clients.size() > 1) {
          final String header;
          if (target.isPresent()) {
            final List<URI> endpoints = target.get().getEndpointSupplier().get();
            header = format(" %s (%s)", target.get().getName(), endpoints);
          } else {
            header = "";
          }
          out.printf("---%s%n", header);
        }
        showReport(out, exact, prefixes, jobIds, formatter, cc.getClient());
      }
      if (out.checkError()) {
        break;
      }
      Thread.sleep(1000 * interval);
    }
  }

  private static void showReport(PrintStream out, boolean exact, final List<String> prefixes,
      final Set<JobId> jobIds, final DateTimeFormatter formatter, final HeliosClient client)
      throws ExecutionException, InterruptedException {
    final Map<JobId, JobStatus> statuses = getStatuses(client, jobIds);

    for (final JobId jobId : jobIds) {
      final JobStatus jobStatus = statuses.get(jobId);
      if (jobStatus == null) {
        continue;
      }
      final Map<String, TaskStatus> taskStatuses = jobStatus.getTaskStatuses();
      if (exact) {
        for (final String host : prefixes) {
          final TaskStatus ts = taskStatuses.get(host);
          out.printf("%-20s %-30s %-8s %s%n",
              chop(jobId.toShortString(), 20),
              chop(host, 30),
              ts != null ? ts.getState() : "UNKNOWN",
              ts != null ? ts.getThrottled() : "UNKNOWN");
        }
      } else {
        for (final String host : taskStatuses.keySet()) {
          if (!hostMatches(prefixes, host)) {
            continue;
          }
          final TaskStatus ts = taskStatuses.get(host);
          out.printf("%-20s %-30s %-8s %s%n",
            chop(jobId.toShortString(), 20),
            chop(host, 30), ts.getState(), ts.getThrottled());
        }
      }
    }
  }

  private static boolean hostMatches(final List<String> prefixes, final String host) {
    if (prefixes.isEmpty()) {
      return true;
    }
    for (String prefix : prefixes) {
      if (host.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static String chop(String s, int len) {
    if (s.length() <= len) {
      return s;
    }
    return s.substring(0, len);
  }

  private static Map<JobId, JobStatus> getStatuses(final HeliosClient client,
                                                   final Set<JobId> jobIds)
      throws ExecutionException, InterruptedException {
    final Map<JobId, ListenableFuture<JobStatus>> futures = getJobsStatuses(client, jobIds);

    final Map<JobId, JobStatus> statuses = Maps.newTreeMap();
    statuses.putAll(allAsMap(futures));
    return statuses;
  }
}
