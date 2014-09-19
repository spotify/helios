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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Joiner.on;
import static com.spotify.helios.cli.Output.table;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;
  private final Argument fullArg;

  public JobListCommand(final Subparser parser) {
    super(parser);

    parser.help("list jobs");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .help("Job reference to filter on");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full job id's.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id's");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final boolean full = options.getBoolean(fullArg.getDest());
    final boolean quiet = options.getBoolean(quietArg.getDest());
    final String pattern = options.getString(patternArg.getDest());

    final Map<JobId, Job> jobs;
    if (pattern == null) {
      jobs = client.jobs().get();
    } else {
      jobs = client.jobs(pattern).get();
    }

    if (!Strings.isNullOrEmpty(pattern) && jobs.isEmpty()) {
      out.printf("job pattern %s matched no jobs%n", pattern);
      return 1;
    }

    final SortedSet<JobId> sortedJobIds = Sets.newTreeSet(jobs.keySet());

    if (json) {
      if (quiet) {
        out.println(Json.asPrettyStringUnchecked(sortedJobIds));
      } else {
        out.println(Json.asPrettyStringUnchecked(jobs));
      }
    } else {
      if (quiet) {
        for (final JobId jobId : sortedJobIds) {
          out.println(jobId);
        }
      } else {
        final Table table = table(out);
        table.row("JOB ID", "NAME", "VERSION", "HOSTS", "COMMAND", "ENVIRONMENT");

        final Map<JobId, ListenableFuture<JobStatus>> statuses = Maps.newTreeMap();
        for (final JobId jobId : sortedJobIds) {
          statuses.put(jobId, client.jobStatus(jobId));
        }

        for (final Map.Entry<JobId, ListenableFuture<JobStatus>> e : statuses.entrySet()) {
          final JobId jobId = e.getKey();
          final Job job = jobs.get(jobId);
          final String command = on(' ').join(escape(job.getCommand()));
          final String env = Joiner.on(" ").withKeyValueSeparator("=").join(job.getEnv());
          final JobStatus status = e.getValue().get();
          table.row(full ? jobId : jobId.toShortString(), jobId.getName(), jobId.getVersion(),
                    status != null ? status.getDeployments().keySet().size() : 0,
                    command, env);
        }
        table.print();
      }
    }

    return 0;
  }

  private static List<String> escape(final List<String> command) {
    final List<String> escaped = Lists.newArrayList();
    for (final String s : command) {
      escaped.add(escape(s));
    }
    return escaped;
  }

  private static String escape(final String arg) {
    return WHITESPACE.matchesAnyOf(arg) ? '"' + arg + '"' : arg;
  }
}

