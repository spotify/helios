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
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class JobRemoveCommand extends WildcardJobCommand {

  private final Argument forceArg;

  public JobRemoveCommand(Subparser parser) {
    super(parser);

    parser.help("remove a job");

    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Force removal.");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId,
                             final BufferedReader stdin)
      throws IOException, ExecutionException, InterruptedException {
    final boolean force = options.getBoolean(forceArg.getDest());

    if (!force) {
      out.printf("This will remove the job %s%n", jobId);
      out.printf("Do you want to continue? [y/N]%n");

      final String line = stdin.readLine().trim();

      if (line.length() < 1) {
        return 1;
      }
      final char c = line.charAt(0);

      if (c != 'Y' && c != 'y') {
        return 1;
      }
    }

    out.printf("Removing job %s%n", jobId);

    int code = 0;

    final JobDeleteResponse response = client.deleteJob(jobId).get();
    out.printf("%s: ", jobId);
    if (response.getStatus() == JobDeleteResponse.Status.OK) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", response);
      code = 1;
    }
    return code;
  }
}
