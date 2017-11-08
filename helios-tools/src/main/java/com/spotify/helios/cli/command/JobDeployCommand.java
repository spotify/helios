/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli.command;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeployResponse;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class JobDeployCommand extends WildcardJobCommand {

  private final Argument hostsArg;
  private final Argument tokenArg;
  private final Argument noStartArg;
  private final Argument watchArg;
  private final Argument intervalArg;

  public JobDeployCommand(final Subparser parser) {
    super(parser);

    parser.help("deploy a job to hosts");

    hostsArg = parser.addArgument("hosts")
        .nargs("+")
        .help("The hosts to deploy the job on.");

    tokenArg = parser.addArgument("--token")
        .nargs("?")
        .setDefault("")
        .help("Insecure access token");

    noStartArg = parser.addArgument("--no-start")
        .action(storeTrue())
        .help("Deploy job without starting it.");

    watchArg = parser.addArgument("--watch")
        .action(storeTrue())
        .help("Watch the newly deployed job (like running job watch right after)");

    intervalArg = parser.addArgument("--interval")
        .setDefault(1)
        .help("if --watch is specified, the polling interval, default 1 second");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId,
                             final BufferedReader stdin)
      throws ExecutionException, InterruptedException {
    final List<String> hosts = options.getList(hostsArg.getDest());
    final Deployment job = Deployment.of(jobId,
        options.getBoolean(noStartArg.getDest()) ? STOP : START);

    if (!json) {
      out.printf("Deploying %s on %s%n", job, hosts);
    }

    int code = 0;

    final HostResolver resolver = HostResolver.create(client);

    final List<String> resolvedHosts = Lists.newArrayList();
    for (final String candidateHost : hosts) {
      final String host = resolver.resolveName(candidateHost);
      resolvedHosts.add(host);
      if (!json) {
        out.printf("%s: ", host);
      }
      final String token = options.getString(tokenArg.getDest());
      final JobDeployResponse result = client.deploy(job, host, token).get();
      if (result.getStatus() == JobDeployResponse.Status.OK) {
        if (!json) {
          out.printf("done%n");
        } else {
          out.print(result.toJsonString());
        }
      } else {
        if (!json) {
          out.printf("failed: %s%n", result);
        } else {
          out.print(result.toJsonString());
        }
        code = 1;
      }
    }

    if (code == 0 && options.getBoolean(watchArg.getDest())) {
      JobWatchCommand.watchJobsOnHosts(out, true, resolvedHosts, ImmutableList.of(jobId),
          options.getInt(intervalArg.getDest()), client);
    }
    return code;
  }
}
