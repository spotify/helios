/*
 * Copyright (c) 2015 Spotify AB.
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.RollingUpdateResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class RollingUpdateCommand extends WildcardJobCommand {

  private final static long POLL_INTERVAL_MILLIS = 1000;

  private final Argument nameArg;
  private final Argument timeoutArg;
  private final Argument parallelismArg;
  private final Argument asyncArg;
  private final Argument rolloutTimeoutArg;

  public RollingUpdateCommand(final Subparser parser) {
    super(parser);

    parser.help("Initiate a rolling update");

    nameArg = parser.addArgument("name")
        .required(true)
        .help("Deployment group name");

    timeoutArg = parser.addArgument("-t", "--timeout")
        .setDefault(RolloutOptions.DEFAULT_TIMEOUT)
        .type(Long.class)
        .help("Fail rollout if a job takes longer than this to reach RUNNING (seconds)");

    parallelismArg = parser.addArgument("-p", "--par")
        .dest("parallelism")
        .setDefault(RolloutOptions.DEFAULT_PARALLELISM)
        .type(Integer.class)
        .help("Number of hosts to deploy to concurrently");

    asyncArg = parser.addArgument("--async")
        .action(storeTrue())
        .help("Don't block until rolling-update is complete");

    rolloutTimeoutArg = parser.addArgument("-T", "--rollout-timeout")
        .setDefault(60L)
        .type(Long.class)
        .help("Exit if rolling-update takes longer than the given value (minutes)");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId,
                             final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());
    final long timeout = options.getLong(timeoutArg.getDest());
    final int parallelism = options.getInt(parallelismArg.getDest());
    final boolean async = options.getBoolean(asyncArg.getDest());
    final long rolloutTimeout = options.getLong(rolloutTimeoutArg.getDest());

    checkArgument(timeout > 0, "Timeout must be greater than 0");
    checkArgument(parallelism > 0, "Parallelism must be greater than 0");
    checkArgument(rolloutTimeout > 0, "Rollout timeout must be greater than 0");

    final long startTime = System.currentTimeMillis();

    final RolloutOptions rolloutOptions = RolloutOptions.newBuilder()
        .setTimeout(timeout)
        .setParallelism(parallelism)
        .build();
    final RollingUpdateResponse response = client.rollingUpdate(name, jobId, rolloutOptions).get();

    if (response.getStatus() != RollingUpdateResponse.Status.OK) {
      if (!json) {
        out.println("Failed: " + response);
      } else {
        out.println(response.toJsonString());
      }
      return 1;
    }

    if (!json) {
      out.println(format("Rolling update%s started: %s -> %s (parallelism=%d, timeout=%d)\n",
                         async ? " (async)" : "",
                         name, jobId.toShortString(), parallelism, timeout));
    }

    if (async) {
      if (json) {
        out.println(response.toJsonString());
      }
      return 0;
    }

    String error = "";
    boolean failed = false;
    boolean timedOut = false;
    final Set<String> reported = Sets.newHashSet();
    while (true) {
      final DeploymentGroupStatusResponse status = client.deploymentGroupStatus(name).get();

      if (status == null) {
        failed = true;
        error = "Failed to fetch deployment-group status";
        break;
      }

      if (status.getJobId() != jobId) {
        // Another rolling-update was started, overriding this one -- exit
        failed = true;
        error = "Deployment-group job id changed during rolling-update";
        break;
      }

      if (!json) {
        for (DeploymentGroupStatusResponse.HostStatus hostStatus : status.getHostStatuses()) {
          final JobId hostJobId = hostStatus.getJobId();
          final String host = hostStatus.getHost();
          final TaskStatus.State state = hostStatus.getState();
          final boolean done = hostJobId != null &&
                               hostJobId.equals(jobId) &&
                               state == TaskStatus.State.RUNNING;

          if (done && reported.add(host)) {
            out.println(format("%s -> %s (%d/%d)", host, state,
                               reported.size(), status.getHostStatuses().size()));
          }
        }
      }

      if (status.getStatus() != DeploymentGroupStatusResponse.Status.ROLLING_OUT) {
        if (status.getStatus() == DeploymentGroupStatusResponse.Status.FAILED) {
          failed = true;
          error = status.getError();
        }
        break;
      }

      if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(rolloutTimeout)) {
        // Rollout timed out
        timedOut = true;
        break;
      }

      Thread.sleep(POLL_INTERVAL_MILLIS);
    }

    final double duration = (System.currentTimeMillis() - startTime) / 1000.0;

    if (json) {
      final Map<String, Object> output = Maps.newHashMap();
      if (failed) {
        output.put("status", "FAILED");
        output.put("error", error);
      } else if (timedOut) {
        output.put("status", "TIMEOUT");
      } else {
        output.put("status", "DONE");
      }
      output.put("duration", duration);
      output.put("parallelism", parallelism);
      output.put("timeout", timeout);
      out.println(Json.asStringUnchecked(output));
    } else {
      out.println();
      if (failed) {
        out.println(format("Failed: %s", error));
      } else if (timedOut) {
        out.println("Timed out! (rolling-update still in progress)");
      } else {
        out.println("Done.");
      }
      out.println(format("Duration: %.2f s", duration));
    }

    return (failed || timedOut) ? 1 : 0;
  }
}

