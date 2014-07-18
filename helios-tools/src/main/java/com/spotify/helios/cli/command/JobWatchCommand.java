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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.cli.Utils.allAsMap;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobWatchCommand extends ControlCommand {

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
  int run(Namespace options, HeliosClient client, PrintStream out, boolean json)
      throws ExecutionException, InterruptedException, IOException {
    boolean exact = options.getBoolean(exactArg.getDest());
    final List<String> prefixes = options.getList(prefixesArg.getDest());
    final String jobIdString = options.getString(jobsArg.getDest());
    final List<ListenableFuture<Map<JobId, Job>>> jobIdFutures = Lists.newArrayList();
    jobIdFutures.add(client.jobs(jobIdString));

    final List<JobId> jobIds = Lists.newArrayList();
    for (ListenableFuture<Map<JobId, Job>> future : jobIdFutures) {
      jobIds.addAll(future.get().keySet());
    }

    watchJobsOnHosts(out, exact, prefixes, jobIds, options.getInt(intervalArg.getDest()),
        client);
    return 0;
  }

  static void watchJobsOnHosts(PrintStream out, boolean exact, final List<String> prefixes,
      final List<JobId> jobIds, final int interval, final HeliosClient client)
      throws InterruptedException, ExecutionException {
    out.println("Control-C to stop");
    out.println("JOB                  HOST                           STATE    THROTTLED?");
    final DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
    while (true) {
      final Map<JobId, JobStatus> statuses = getStatuses(client, jobIds);

      final Instant now = new Instant();
      out.printf("-------------------- ------------------------------ -------- "
          + "---------- [%s UTC]%n", now.toString(formatter));
      for (final JobId jobId : jobIds) {
        final JobStatus jobStatus = statuses.get(jobId);
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
      if (out.checkError()) {
        break;
      }
      Thread.sleep(1000 * interval);
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

  private static Map<JobId, JobStatus> getStatuses(HeliosClient client, final List<JobId> jobIds)
      throws ExecutionException, InterruptedException {
    final Map<JobId, ListenableFuture<JobStatus>> futures = Maps.newTreeMap();
    for (final JobId jobId : jobIds) {
      futures.put(jobId, client.jobStatus(jobId));
    }
    final Map<JobId, JobStatus> statuses = Maps.newTreeMap();
    statuses.putAll(allAsMap(futures));
    return statuses;
  }
}
