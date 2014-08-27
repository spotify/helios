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

import com.google.common.collect.ImmutableList;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class JobUndeployCommand extends WildcardJobCommand {

  private final Argument hostsArg;
  private final Argument allArg;
  private final Argument forceArg;

  public JobUndeployCommand(final Subparser parser) {
    super(parser);

    parser.help("undeploy a job from hosts");

    allArg = parser.addArgument("-a", "--all")
        .action(storeTrue())
        .help("Undeploy from all currently deployed hosts.");

    forceArg = parser.addArgument("-f", "--force")
        .action(storeTrue())
        .help("Force yes. Immediately undeploy job without prompting. " +
              "Useful together with -a/--all. Please use with care.");

    hostsArg = parser.addArgument("hosts")
        .nargs("*")
        .help("The hosts to undeploy the job from.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId)
      throws ExecutionException, InterruptedException, IOException {

    final boolean all = options.getBoolean(allArg.getDest());
    final boolean force = options.getBoolean(forceArg.getDest());
    final List<String> hosts;

    if (all) {
      final JobStatus status = client.jobStatus(jobId).get();
      hosts = ImmutableList.copyOf(status.getDeployments().keySet());
      if (hosts.isEmpty()) {
        out.printf("%s is not currently deployed on any hosts.", jobId);
        return 0;
      }

      if (!force) {
        out.printf("This will undeploy %s from %s%n", jobId, hosts);
        out.printf("Do you want to continue? [y/N]%n");

        // TODO (dano): pass in stdin instead using System.in
        final int c = System.in.read();

        if (c != 'Y' && c != 'y') {
          return 1;
        }
      }
    } else {
      hosts = options.getList(hostsArg.getDest());
      if (hosts.isEmpty()) {
        out.println("Please either specify a list of hosts or use the -a/--all flag.");
        return 1;
      }
    }

    if (!json) {
      out.printf("Undeploying %s from %s%n", jobId, hosts);
    }

    int code = 0;
    final HostResolver resolver = HostResolver.create(client);

    for (final String candidateHost : hosts) {
      final String host = resolver.resolveName(candidateHost);

      if (!json) {
        out.printf("%s: ", host);
      }

      final JobUndeployResponse response = client.undeploy(jobId, host).get();
      if (response.getStatus() == JobUndeployResponse.Status.OK) {
        if (!json) {
          out.println("done");
        } else {
          out.printf(response.toJsonString());
        }
      } else {
        if (!json) {
          out.println("failed: " + response);
        } else {
          out.printf(response.toJsonString());
        }
        code = -1;
      }
    }

    return code;
  }
}
