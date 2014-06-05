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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.SetGoalResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobStartCommand extends WildcardJobCommand {

  private final Argument hostsArg;

  public JobStartCommand(Subparser parser) {
    super(parser);

    parser.help("start a stopped job");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to start the job on.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId)
      throws ExecutionException, InterruptedException, IOException {

    final List<String> hosts = options.getList(hostsArg.getDest());

    final Deployment deployment = new Deployment.Builder()
        .setGoal(Goal.START)
        .setJobId(jobId)
        .build();

    out.printf("Starting %s on %s%n", jobId, hosts);

    int code = 0;

    for (final String host : hosts) {
      out.printf("%s: ", host);
      final SetGoalResponse result = client.setGoal(deployment, host).get();
      if (result.getStatus() == SetGoalResponse.Status.OK) {
        out.printf("done%n");
      } else {
        out.printf("failed: %s%n", result);
        code = 1;
      }
    }

    return code;
  }
}
