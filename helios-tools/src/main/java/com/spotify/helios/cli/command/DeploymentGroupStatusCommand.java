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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.helios.cli.DeploymentGroupStatusTable;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutTask;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class DeploymentGroupStatusCommand extends ControlCommand {

  private final Argument nameArg;
  private final Argument fullArg;

  public DeploymentGroupStatusCommand(final Subparser parser) {
    super(parser);

    parser.help("show job or host status");

    nameArg = parser.addArgument("name")
        .help("Deployment group name");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full hostnames, job and container id's.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final String name = options.getString(nameArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());

    if (name == null) {
      throw new IllegalArgumentException("Please specify a deployment group name.");
    }

    final DeploymentGroupStatus status = client.deploymentGroupStatus(name).get();

    if (status == null) {
      out.printf("Unknown deployment group: %s%n", name);
      return 1;
    }

    if (json) {
      out.println(Json.asPrettyStringUnchecked(status));
    } else {
      final JobId jobId = status.getDeploymentGroup().getJob();
      final List<RolloutTask> rolloutTasks = status.getRolloutTasks();
      final List<String> hosts = getHosts(rolloutTasks);
      final int hostIndex = status.getHostIndex();
      final String dgName = status.getDeploymentGroup().getName();
      final String error = status.getError();

      out.printf("Name: %s%n", dgName);
      out.printf("Job: %s%n", jobId);
      out.printf("State: %s%n", status.getDisplayState());

      if (!Strings.isNullOrEmpty(error)) {
        out.printf("Error: %s - %s%n%n", hosts.get(hostIndex), error);
      } else {
        out.printf("%n");
      }

      new DeploymentGroupStatusTable(out, hosts, hostIndex, full).print();
    }

    return 0;
  }

  private static List<String> getHosts(final List<RolloutTask> rolloutTasks) {
    final Set<String> uniqueHosts = Sets.newLinkedHashSet();
    for (RolloutTask task : rolloutTasks) {
      uniqueHosts.add(task.getTarget());
    }
    return Lists.newArrayList(uniqueHosts);
  }
}
