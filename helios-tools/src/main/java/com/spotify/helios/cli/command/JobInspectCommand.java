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
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TcpHealthCheck;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.CharMatcher.WHITESPACE;

public class JobInspectCommand extends WildcardJobCommand {

  private static final Function<String, String> QUOTE = new Function<String, String>() {
    @Override
    public String apply(final String input) {
      return quote(input);
    }
  };

  private static final Function<PortMapping, String> FORMAT_PORTMAPPING =
      new Function<PortMapping, String>() {
        @Override
        public String apply(final PortMapping input) {
          String s = String.valueOf(input.getInternalPort());
          if (input.getExternalPort() != null) {
            s += ":" + input.getExternalPort();
          }
          if (input.getProtocol() != null) {
            s += "/" + input.getProtocol();
          }
          return s;
        }
      };

  private static final Function<ServicePorts, String> FORMAT_SERVICE_PORTS =
      new Function<ServicePorts, String>() {
        @Override
        public String apply(final ServicePorts input) {
          return Joiner.on(", ").join(Ordering.natural().sortedCopy(input.getPorts().keySet()));
        }
      };

  private static String formatHealthCheck(final HealthCheck healthCheck) {
    if (healthCheck == null) {
      return "";
    }
    String s = String.format("type: %s", String.valueOf(healthCheck.getType()));
    if (healthCheck instanceof HttpHealthCheck) {
      final HttpHealthCheck httpHealthCheck = (HttpHealthCheck) healthCheck;
      s += String.format(", port: %s, path: %s", httpHealthCheck.getPort(),
                         httpHealthCheck.getPath());
    } else if (healthCheck instanceof TcpHealthCheck) {
      final TcpHealthCheck tcpHealthCheck = (TcpHealthCheck) healthCheck;
      s += String.format(", port: %s", tcpHealthCheck.getPort());
    } else if (healthCheck instanceof ExecHealthCheck) {
      final ExecHealthCheck execHealthCheck = (ExecHealthCheck) healthCheck;
      s += String.format(", command: %s", Joiner.on(" ").join(execHealthCheck.getCommand()));
    }
    return s;
  }

  public JobInspectCommand(final Subparser parser) {
    super(parser);

    parser.help("print the configuration of a job");
  }

  @Override
  protected int runWithJobId(final Namespace options, final HeliosClient client,
                             final PrintStream out, final boolean json, final JobId jobId,
                             final BufferedReader stdin)
      throws ExecutionException, InterruptedException {

    final Map<JobId, Job> jobs = client.jobs(jobId.toString()).get();
    if (jobs.size() == 0) {
      out.printf("Unknown job: %s%n", jobId);
      return 1;
    }

    final Job job = Iterables.getOnlyElement(jobs.values());

    if (json) {
      out.println(Json.asPrettyStringUnchecked(job));
    } else {
      out.printf("Id: %s%n", job.getId());
      out.printf("Image: %s%n", job.getImage());
      out.printf("Hostname: %s%n", job.getHostname());
      out.printf("Command: %s%n", quote(job.getCommand()));
      printMap(out, "Env:   ", QUOTE, job.getEnv());
      out.printf("Health check: %s%n", formatHealthCheck(job.getHealthCheck()));
      out.printf("Grace period (seconds): %s%n", job.getGracePeriod());
      printMap(out, "Ports: ", FORMAT_PORTMAPPING, job.getPorts());
      printMap(out, "Reg: ", FORMAT_SERVICE_PORTS, job.getRegistration());
      out.printf("Security options: %s%n", job.getSecurityOpt());
      out.printf("Network mode: %s%n", job.getNetworkMode());
      out.printf("Token: %s%n", job.getToken());
      printVolumes(out, job.getVolumes());
    }

    return 0;
  }

  private <K extends Comparable<K>, V> void printMap(final PrintStream out, final String name,
                                                     final Function<V, String> transform,
                                                     final Map<K, V> values) {
    out.print(name);
    boolean first = true;
    for (final K key : Ordering.natural().sortedCopy(values.keySet())) {
      if (!first) {
        out.print(Strings.repeat(" ", name.length()));
      }
      final V value = values.get(key);
      out.printf("%s=%s%n", key, transform.apply(value));
      first = false;
    }
    if (first) {
      out.println();
    }
  }

  private void printVolumes(final PrintStream out, final Map<String, String> volumes) {
    final String prefix = "Volumes: ";
    out.print(prefix);
    boolean first = true;
    for (Map.Entry<String, String> entry : volumes.entrySet()) {
      if (!first) {
        out.print(Strings.repeat(" ", prefix.length()));
      }
      final String path = entry.getValue();
      final String source = entry.getKey();
      if (source == null) {
        out.printf("%s%n", path);
      } else {
        // Note that we're printing this in value:key order as that's the host:container:[rw|ro]
        // order used by docker and the helios create command.
        out.printf("%s:%s%n", path, source);
      }
      first = false;
    }
    if (first) {
      out.println();
    }
  }

  private static String quote(final String s) {
    if (s == null) {
      return "";
    }
    return WHITESPACE.matchesAnyOf(s)
           ? '"' + s + '"'
           : s;
  }

  private static List<String> quote(final List<String> ss) {
    final List<String> output = Lists.newArrayList();
    for (String s : ss) {
      output.add(quote(s));
    }
    return output;
  }
}
