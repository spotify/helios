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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Ordering.natural;
import static com.spotify.helios.cli.Output.formatHostname;
import static com.spotify.helios.cli.Output.humanDuration;
import static com.spotify.helios.cli.Output.table;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.helios.cli.Table;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DockerVersion;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class HostListCommand extends ControlCommand {

  private final Argument quietArg;
  private final Argument patternArg;
  private final Argument fullArg;
  private final Argument statusArg;
  private final Argument hostSelectorsArg;

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

    hostSelectorsArg = parser.addArgument("-s", "--selector")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Host selector expression. The list of hosts will be filtered to match only those "
              + "whose labels match all of the supplied expressions. "
              + "Multiple selector expressions can be specified with multiple `-s` arguments "
              + "(e.g. `-s site=foo -s bar!=yes`). "
              + "Supported operators are '=', '!=', 'in' and 'notin'.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {

    final String pattern = options.getString(patternArg.getDest());
    final List<String> selectorArgValue = options.getList(hostSelectorsArg.getDest());
    final Set<String> selectors = ImmutableSet.copyOf(selectorArgValue);

    final List<String> hosts;

    if (pattern.isEmpty() && selectors.isEmpty()) {
      hosts = client.listHosts().get();
    } else if (!pattern.isEmpty() && selectors.isEmpty()) {
      hosts = client.listHosts(pattern).get();
    } else if (pattern.isEmpty() && !selectors.isEmpty()) {
      hosts = client.listHosts(selectors).get();
    } else {
      hosts = client.listHosts(pattern, selectors).get();
    }

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

    if (hosts.isEmpty()) {
      if (json) {
        out.println("{ }");
      } else if (!quiet && !isNullOrEmpty(pattern)) {
        out.printf("host pattern %s matched no hosts%n", pattern);
      }
      return 1;
    }

    if (quiet) {
      final List<String> sortedHosts = natural().sortedCopy(hosts);
      if (json) {
        out.println(Json.asPrettyStringUnchecked(sortedHosts));
      } else {
        for (final String host : sortedHosts) {
          out.println(formatHostname(full, host));
        }
      }
    } else {
      final Map<String, HostStatus> statuses =
          new TreeMap<>(client.hostStatuses(hosts, queryParams).get());

      if (json) {
        out.println(Json.asPrettyStringUnchecked(statuses));
      } else {
        final Table table = table(out);
        table.row("HOST", "STATUS", "DEPLOYED", "RUNNING", "CPUS", "MEM", "LOAD AVG", "MEM USAGE",
            "OS", "HELIOS", "DOCKER", "LABELS");

        for (final Map.Entry<String, HostStatus> e : statuses.entrySet()) {

          final String host = e.getKey();
          final HostStatus s = e.getValue();

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

          final String hostLabels = Joiner.on(", ").withKeyValueSeparator("=").join(s.getLabels());

          table.row(formatHostname(full, host), status, s.getJobs().size(),
              runningDeployedJobs.size(), cpus, mem, loadAvg, memUsage, os, version, docker,
              hostLabels);
        }

        table.print();
      }
    }
    return 0;
  }
}
