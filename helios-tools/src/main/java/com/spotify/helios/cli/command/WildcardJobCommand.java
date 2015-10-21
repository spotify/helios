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

import com.google.common.collect.Iterables;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeployResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

abstract class WildcardJobCommand extends ControlCommand {

  private final Argument jobArg;

  public WildcardJobCommand(final Subparser parser) {
    this(parser, false);
  }

  public WildcardJobCommand(final Subparser parser, final boolean shortCircuit) {
    super(parser, shortCircuit);

    jobArg = parser.addArgument("job")
        .help("Job id.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final String jobIdString = options.getString(jobArg.getDest());
    final Map<JobId, Job> jobs = client.jobs(jobIdString).get();

    if (jobs.size() == 0) {
      if (!json) {
        out.printf("Unknown job: %s%n", jobIdString);
      } else {
        JobDeployResponse jobDeployResponse =
            new JobDeployResponse(JobDeployResponse.Status.JOB_NOT_FOUND, null, null);
        out.printf(jobDeployResponse.toJsonString());
      }
      return 1;
    } else if (jobs.size() > 1) {
      if (!json) {
        out.printf("Ambiguous job reference: %s%n", jobIdString);
      } else {
        JobDeployResponse jobDeployResponse =
            new JobDeployResponse(JobDeployResponse.Status.AMBIGUOUS_JOB_REFERENCE, null, null);
        out.printf(jobDeployResponse.toJsonString());
      }
      return 1;
    }

    final JobId jobId = Iterables.getOnlyElement(jobs.keySet());

    return runWithJobId(options, client, out, json, jobId, stdin);
  }

  protected abstract int runWithJobId(final Namespace options, final HeliosClient client,
                                      final PrintStream out, final boolean json, final JobId jobId,
                                      final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException;
}
