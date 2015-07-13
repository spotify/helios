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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.cli.Output.formatHostname;
import static com.spotify.helios.cli.Output.table;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class DeploymentGroupStatusCommand extends ControlCommand {

  private final Argument nameArg;
  private final Argument fullArg;

  public DeploymentGroupStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("Show deployment-group status");

    nameArg = parser.addArgument("name")
        .required(true)
        .help("Deployment group name");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full hostnames and job ids.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final String name = options.getString(nameArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());

    return run0(client, out, json, name, full);
  }

  static int run0(final HeliosClient client, final PrintStream out, final boolean json,
                          final String name, final boolean full)
      throws ExecutionException, InterruptedException {
    final DeploymentGroupStatusResponse status = client.deploymentGroupStatus(name).get();

    if (status == null) {
      if (json) {
        final Map<String, Object> output = Maps.newHashMap();
        output.put("status", "DEPLOYMENT_GROUP_NOT_FOUND");
        out.print(Json.asStringUnchecked(output));
      } else {
        out.printf("Unknown deployment group: %s%n", name);
      }
      return 1;
    }

    if (json) {
      out.println(Json.asPrettyStringUnchecked(status));
    } else {
      final JobId jobId = status.getJobId();
      final String error = status.getError();

      out.printf("Name: %s%n", name);
      out.printf("Job Id: %s%n", full ? jobId : jobId.toShortString());
      out.printf("Status: %s%n", status.getStatus());

      if (!Strings.isNullOrEmpty(error)) {
        out.printf("Error: %s%n", error);
      }
      out.printf("%n");

      printTable(out, jobId, status.getHostStatuses(), full);
    }

    return 0;
  }

  private static void printTable(final PrintStream out,
                                 final JobId jobId,
                                 final List<DeploymentGroupStatusResponse.HostStatus> hosts,
                                 final boolean full) {
    final Table table = table(out);
    table.row("HOST", "UP-TO-DATE", "JOB", "STATE");

    for (final DeploymentGroupStatusResponse.HostStatus hostStatus : hosts) {
      final String displayHostName = formatHostname(full, hostStatus.getHost());

      final boolean upToDate = hostStatus.getJobId() != null &&
                               hostStatus.getJobId().equals(jobId);

      final String job;
      if (hostStatus.getJobId() == null) {
        job = "-";
      } else if (full) {
        job = hostStatus.getJobId().toString();
      } else {
        job = hostStatus.getJobId().toShortString();
      }

      final String state = hostStatus.getState() != null ?
                           hostStatus.getState().toString() : "-";

      table.row(displayHostName, upToDate ? "X" : "", job, state);
    }

    table.print();
  }
}
