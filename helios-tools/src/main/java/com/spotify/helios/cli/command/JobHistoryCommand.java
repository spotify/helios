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

import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getLast;
import static com.spotify.helios.cli.Output.table;

public class JobHistoryCommand extends ControlCommand {

  private final Argument jobIdArg;

  public JobHistoryCommand(Subparser parser) {
    super(parser);

    parser.help("see the run history of a job");

    jobIdArg = parser.addArgument("jobid")
         .help("Job id");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {

    String jobIdString = options.getString(jobIdArg.getDest());

    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (jobs.size() == 0) {
      out.printf("Unknown job: %s%n", jobIdString);
      return 1;
    } else if (jobs.size() > 1) {
      out.printf("Ambiguous job id: %s%n", jobIdString);
      return 1;
    }

    final JobId jobId = getLast(jobs.keySet());

    TaskStatusEvents result = client.jobHistory(jobId).get();

    if (json) {
      out.println(Json.asPrettyStringUnchecked(result));
      return 0;
    }

    final Table table = table(out);
    table.row("HOST", "TIMESTAMP", "STATE", "THROTTLED", "CONTAINERID");
    List<TaskStatusEvent> events = result.getEvents();
    DateTimeFormatter format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS");

    for (TaskStatusEvent event : events) {
      String host = checkNotNull(event.getHost());
      long timestamp = checkNotNull(event.getTimestamp());
      TaskStatus status = checkNotNull(event.getStatus());
      State state = checkNotNull(status.getState());
      String containerId = status.getContainerId();
      containerId = containerId == null ? "<none>" : containerId;

      table.row(host, format.print(timestamp), state, status.getThrottled(), containerId);
    }
    table.print();
    return 0;
  }
}
