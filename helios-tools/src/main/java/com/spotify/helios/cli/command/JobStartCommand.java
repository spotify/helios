/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.JobId;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JobStartCommand extends WildcardJobCommand {

  private final Argument hostsArg;
  private final Argument tokenArg;

  public JobStartCommand(Subparser parser) {
    super(parser);

    parser.help("start a stopped job");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to start the job on.");

    tokenArg = parser.addArgument("--token")
        .nargs("?")
        .setDefault("")
        .help("Insecure access token");
 }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId,
                             final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final List<String> hosts = options.getList(hostsArg.getDest());

    final Deployment deployment = new Deployment.Builder()
        .setGoal(Goal.START)
        .setJobId(jobId)
        .build();

    if (!json) {
      out.printf("Starting %s on %s%n", jobId, hosts);
    }

    return Utils.setGoalOnHosts(client, out, json, hosts, deployment,
        options.getString(tokenArg.getDest()));
  }
}
