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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.authentication.HeliosAuthException;
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

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Ordering.natural;
import static com.spotify.helios.cli.Output.formatHostname;
import static com.spotify.helios.cli.Output.humanDuration;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.cli.Utils.allAsMap;
import static com.spotify.helios.cli.Utils.argToStringMap;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class HostListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;
  private final Argument fullArg;
  private final Argument statusArg;
  private final Argument labelsArg;

  private final String statusChoicesString;

  public HostListCommand(final Subparser parser) {
    super(parser);

    Collection<String> statusChoices = Collections2.transform(
        Arrays.asList(HostStatus.Status.values()), new Function<HostStatus.Status, String>() {
          @Override
          public String apply(final HostStatus.Status input) {
            return input.toString();
          }
        });

    statusChoicesString = Joiner.on(", ").join(statusChoices);

    parser.help("list hosts");

    patternArg = parser.addArgument("pattern")
        .nargs("?")
        .setDefault("")
        .help("Pattern to filter hosts with");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print host names");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full host names.");

    statusArg = parser.addArgument("--status")
        .nargs("?")
        .choices(statusChoices.toArray(new String[statusChoices.size()]))
        .help("Filter hosts by its status. Valid statuses are: " + statusChoicesString);

    labelsArg = parser.addArgument("-l", "--labels")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Only include hosts that match all of these labels. Labels need to be in the format "
              + "key=value.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, HeliosAuthException {
    final String pattern = options.getString(patternArg.getDest());
    final List<String> hosts = FluentIterable
        .from(client.listHosts().get())
        .filter(containsPattern(pattern))
        .toList();

    final Map<String, String> queryParams = Maps.newHashMap();
    final String statusFilter = options.getString(statusArg.getDest());
    if (!isNullOrEmpty(statusFilter)) {
      try {
        HostStatus.Status.valueOf(statusFilter);
        queryParams.put("status", statusFilter);
      } catch (IllegalArgumentException ignored) {
        throw new IllegalArgumentException(
            "Invalid status. Valid statuses are: " + statusChoicesString);
      }
    }

    final boolean full = options.getBoolean(fullArg.getDest());
    final boolean quiet = options.getBoolean(quietArg.getDest());

    if (!isNullOrEmpty(pattern) && hosts.isEmpty()) {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(hosts));
      } else if (!quiet) {
        out.printf("host pattern %s matched no hosts%n", pattern);
      }
      return 1;
    }

    final List<String> sortedHosts = natural().sortedCopy(hosts);

    final Map<String, String> selectedLabels;
    try {
      selectedLabels = argToStringMap(options, labelsArg);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage() +
                                         "\nLabels need to be in the format key=value.");
    }

    if (selectedLabels != null && !selectedLabels.isEmpty() && json) {
      System.err.println("Warning: filtering by label is not supported for JSON output. Not doing"
                         + " any filtering by label.");
    }

    if (quiet) {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(sortedHosts));
      } else {
        for (final String host : sortedHosts) {
          out.println(formatHostname(full, host));
        }
      }
    } else {
      final Map<String, ListenableFuture<HostStatus>> statuses = Maps.newTreeMap();
      try {
        final Map<String, HostStatus> hostStatuses = client.hostStatuses(hosts, queryParams).get();
        for (final Entry<String, HostStatus> entry : hostStatuses.entrySet()) {
          statuses.put(entry.getKey(), Futures.immediateFuture(entry.getValue()));
        }
      } catch (ExecutionException e) {
        System.err.println("Warning: masters failed batch status fetching.  Falling back to"
                           + " slower host status method");
        for (final String host : hosts) {
          statuses.put(host, client.hostStatus(host, queryParams));
        }
      }
      if (json) {
        final Map<String, HostStatus> sorted = Maps.newTreeMap();
        sorted.putAll(allAsMap(statuses));
        out.println(Json.asPrettyStringUnchecked(sorted));
      } else {
        final Table table = table(out);
        table.row("HOST", "STATUS", "DEPLOYED", "RUNNING", "CPUS", "MEM", "LOAD AVG", "MEM USAGE",
                  "OS", "HELIOS", "DOCKER", "LABELS");

        for (final Map.Entry<String, ListenableFuture<HostStatus>> e : statuses.entrySet()) {

          final String host = e.getKey();
          final HostStatus s = e.getValue().get();

          if (s == null) {
            continue;
          }

          boolean skipHost = false;
          if (selectedLabels != null && !selectedLabels.isEmpty()) {
            final Map<String, String> hostLabels = s.getLabels();
            for (Entry<String, String> label : selectedLabels.entrySet()) {
              final String key = label.getKey();
              final String value = label.getValue();

              if (!hostLabels.containsKey(key)) {
                skipHost = true;
                break;
              }

              final String hostValue = hostLabels.get(key);

              if (isNullOrEmpty(value) ? !isNullOrEmpty(hostValue) : !value.equals(hostValue)) {
                skipHost = true;
                break;
              }
            }
          }
          if (skipHost) {
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
            docker = (dv != null) ? format("%s (%s)", dv.getVersion(), dv.getApiVersion()) : "";
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

          final String labels = Joiner.on(", ").withKeyValueSeparator("=").join(s.getLabels());

          table.row(formatHostname(full, host), status, s.getJobs().size(),
              runningDeployedJobs.size(), cpus, mem, loadAvg, memUsage, os, version, docker,
              labels);
        }

        table.print();
      }
    }
    return 0;
  }
}
