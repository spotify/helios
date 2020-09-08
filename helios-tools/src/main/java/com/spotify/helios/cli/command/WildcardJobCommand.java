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

import com.google.common.collect.Iterables;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeployResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * A ControlCommand that accepts as input a partial jobId argument. An error is returned if there
 * are zero or more than one jobs in the cluster that match this partial jobId.
 */
abstract class WildcardJobCommand extends ControlCommand {

  private final Argument jobArg;
  private final Argument strictStartArg;

  public WildcardJobCommand(final Subparser parser) {
    this(parser, false);
  }

  public WildcardJobCommand(final Subparser parser, final boolean shortCircuit) {
    super(parser, shortCircuit);

    jobArg = parser.addArgument("job")
        .help("Job id.");
    strictStartArg = parser.addArgument("--strict-start")
        .type(Boolean.class)
        .setDefault(false)
        .help("Forces job names to be matched from the string start. "
              + "By default, some commands will match on any jobs that contain the input name as a"
              + " substring, this option forces the match to only happen if the string starts with"
              + " the input name. Affects the subcommands: remove, inspect, rolling-update,"
              + " undeploy, deploy and stop.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String jobIdString = options.getString(jobArg.getDest());
    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (options.getBoolean(strictStartArg.getDest())) {
      for (final Map.Entry<JobId, Job> entry : jobs.entrySet()) {
        if (!entry.getKey().toShortString().startsWith(jobIdString)) {
          jobs.remove(entry.getKey());
        }
      }
    }

    if (jobs.size() == 0) {
      if (!json) {
        out.printf("Unknown job: %s%n", jobIdString);
      } else {
        final JobDeployResponse jobDeployResponse =
            new JobDeployResponse(JobDeployResponse.Status.JOB_NOT_FOUND, null, null);
        out.print(jobDeployResponse.toJsonString());
      }
      return 1;
    } else if (jobs.size() > 1) {
      if (!json) {
        out.printf("Ambiguous job reference: %s%n", jobIdString);
      } else {
        final JobDeployResponse jobDeployResponse =
            new JobDeployResponse(JobDeployResponse.Status.AMBIGUOUS_JOB_REFERENCE, null, null);
        out.print(jobDeployResponse.toJsonString());
      }
      return 1;
    }

    final Job job = Iterables.getOnlyElement(jobs.values());

    return runWithJob(options, client, out, json, job, stdin);
  }

  protected abstract int runWithJob(final Namespace options, final HeliosClient client,
                                    final PrintStream out, final boolean json, final Job job,
                                    final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException;
}
