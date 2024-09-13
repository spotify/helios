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

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.common.collect.ImmutableList;
import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class JobUndeployCommand extends WildcardJobCommand {

  private final Argument hostsArg;
  private final Argument tokenArg;
  private final Argument allArg;
  private final Argument yesArg;

  public JobUndeployCommand(final Subparser parser) {
    super(parser, false);

    parser.help("undeploy a job from hosts");

    hostsArg = parser.addArgument("hosts")
        .nargs("*")
        .help("The hosts to undeploy the job from.");

    tokenArg = parser.addArgument("--token")
        .nargs("?")
        .setDefault("")
        .help("Insecure access token");

    allArg = parser.addArgument("-a", "--all")
        .action(storeTrue())
        .help("Undeploy from all currently deployed hosts.");

    yesArg = parser.addArgument("--yes")
        .action(storeTrue())
        .help("Automatically answer 'yes' to the interactive prompt.");
  }

  @Override
  protected int runWithJob(final Namespace options, final HeliosClient client,
                           final PrintStream out, final boolean json, final Job job,
                           final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final JobId jobId = job.getId();
    final boolean all = options.getBoolean(allArg.getDest());
    final boolean yes = options.getBoolean(yesArg.getDest());
    final List<String> hosts;

    if (all) {
      final JobStatus status = client.jobStatus(jobId).get();
      hosts = ImmutableList.copyOf(status.getDeployments().keySet());
      if (hosts.isEmpty()) {
        out.printf("%s is not currently deployed on any hosts.", jobId);
        return 0;
      }

      if (!yes) {
        out.printf("This will undeploy %s from %s%n", jobId, hosts);
        final boolean confirmed = Utils.userConfirmed(out, stdin);
        if (!confirmed) {
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

      final String token = options.getString(tokenArg.getDest());
      final JobUndeployResponse response = client.undeploy(jobId, host, token).get();
      if (response.getStatus() == JobUndeployResponse.Status.OK) {
        if (!json) {
          out.println("done");
        } else {
          out.print(response.toJsonString());
        }
      } else {
        if (!json) {
          out.println("failed: " + response);
        } else {
          out.print(response.toJsonString());
        }
        code = -1;
      }
    }

    return code;
  }
}
