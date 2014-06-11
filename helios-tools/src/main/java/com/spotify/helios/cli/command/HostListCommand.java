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

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DockerVersion;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.collect.Ordering.natural;
import static com.spotify.helios.cli.Output.humanDuration;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.cli.Utils.allAsMap;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;

  public HostListCommand(final Subparser parser) {
    super(parser);

    parser.help("list hosts");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .setDefault("")
        .help("Pattern to filter hosts with");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print host names");

  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final String pattern = options.getString(patternArg.getDest());
    final List<String> hosts = FluentIterable
        .from(client.listHosts().get())
        .filter(containsPattern(pattern))
        .toList();

    if (!Strings.isNullOrEmpty(pattern) && hosts.isEmpty()) {
      out.printf("host pattern %s matched no hosts%n", pattern);
      return 1;
    }

    final List<String> sortedHosts = natural().sortedCopy(hosts);

    final boolean quiet = options.getBoolean(quietArg.getDest());

    if (quiet) {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(sortedHosts));
      } else {
        for (final String host : sortedHosts) {
          out.println(host);
        }
      }
    } else {
      final Map<String, ListenableFuture<HostStatus>> statuses = Maps.newTreeMap();
      for (final String host : hosts) {
        statuses.put(host, client.hostStatus(host));
      }
      if (json) {
        final Map<String, HostStatus> sorted = Maps.newTreeMap();
        sorted.putAll(allAsMap(statuses));
        out.println(Json.asPrettyStringUnchecked(sorted));
      } else {
        final Table table = table(out);
        table.row("HOST", "STATUS", "DEPLOYED", "RUNNING",
                  "CPUS", "MEM", "LOAD AVG", "MEM USAGE", "OS", "VERSION", "DOCKER");

        for (final Map.Entry<String, ListenableFuture<HostStatus>> e : statuses.entrySet()) {

          final String host = e.getKey();
          final HostStatus s = e.getValue().get();

          if (s == null) {
            continue;
          }

          final Set<TaskStatus> runningDeployedJobs = Sets.newHashSet();
          for (final JobId jobId : s.getJobs().keySet()) {
            final TaskStatus taskStatus = s.getStatuses().get(jobId);
            if (taskStatus == null) {
              continue;
            }
            if (taskStatus.getState() == TaskStatus.State.RUNNING) {
              runningDeployedJobs.add(taskStatus);
            }
          }

          final HostInfo hi = s.getHostInfo();
          final String memUsage;
          final String cpus;
          final String mem;
          final String loadAvg;
          final String os;
          final String docker;
          if (hi != null) {
            final long free = hi.getMemoryFreeBytes();
            final long total = hi.getMemoryTotalBytes();
            memUsage = format("%.2f", (float) (total - free) / total);
            cpus = String.valueOf(hi.getCpus());
            mem = hi.getMemoryTotalBytes() / (1024 * 1024 * 1024) + " gb";
            loadAvg = format("%.2f", hi.getLoadAvg());
            os = hi.getOsName() + " " + hi.getOsVersion();
            final DockerVersion dv = hi.getDockerVersion();
            docker = format("%s (%s)", dv, dv.getApiVersion());
          } else {
            memUsage = cpus = mem = loadAvg = os = docker = "";
          }

          final String version;
          if (s.getAgentInfo() != null) {
            version = Optional.fromNullable(s.getAgentInfo().getVersion()).or("");
          } else {
            version = "";
          }

          String status = s.getStatus() == UP ? "Up" : "Down";
          if (s.getAgentInfo() != null) {
            final long startTime = s.getAgentInfo().getStartTime();
            final long upTime = s.getAgentInfo().getUptime();
            if (s.getStatus() == UP) {
              status += " " + humanDuration(currentTimeMillis() - startTime);
            } else {
              status += " " + humanDuration(currentTimeMillis() - startTime - upTime);
            }
          }

          table.row(host, status, s.getJobs().size(), runningDeployedJobs.size(),
                    cpus, mem, loadAvg, memUsage, os, version, docker);
        }

        table.print();
      }
    }
    return 0;
  }
}
