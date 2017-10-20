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

import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class JobRemoveCommand extends WildcardJobCommand {

  private final Argument tokenArg;
  private final Argument yesArg;

  public JobRemoveCommand(Subparser parser) {
    super(parser);

    parser.help("remove a job");

    tokenArg = parser.addArgument("--token")
        .nargs("?")
        .setDefault("")
        .help("Insecure access token");

    yesArg = parser.addArgument("--yes")
        .action(Arguments.storeTrue())
        .help("Automatically answer 'yes' to the interactive prompt.");
  }

  @Override
  protected int runWithJob(final Namespace options, final HeliosClient client,
                           final PrintStream out, final boolean json, final Job job,
                           final BufferedReader stdin)
      throws IOException, ExecutionException, InterruptedException {
    final boolean yes = options.getBoolean(yesArg.getDest());
    final JobId jobId = job.getId();

    if (!yes) {
      out.printf("This will remove the job %s%n", jobId);
      final boolean confirmed = Utils.userConfirmed(out, stdin);
      if (!confirmed) {
        return 1;
      }
    }

    if (!json) {
      out.printf("Removing job %s%n", jobId);
    }

    int code = 0;

    final String token = options.getString(tokenArg.getDest());
    final JobDeleteResponse response = client.deleteJob(jobId, token).get();
    if (!json) {
      out.printf("%s: ", jobId);
    }
    if (response.getStatus() == JobDeleteResponse.Status.OK) {
      if (json) {
        out.print(response.toJsonString());
      } else {
        out.printf("done%n");
      }
    } else {
      if (json) {
        out.print(response.toJsonString());
      } else {
        out.printf("failed: %s%n", response);
      }
      code = 1;
    }
    return code;
  }
}
