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

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class DeploymentGroupInspectCommand extends ControlCommand {

  private final Argument nameArg;

  public DeploymentGroupInspectCommand(final Subparser parser) {
    super(parser);

    parser.help("inspect a deployment group");

    nameArg = parser.addArgument("name")
        .nargs("?")
        .help("Deployment group name");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final String name = options.getString(nameArg.getDest());

    if (name == null) {
      throw new IllegalArgumentException("Please specify a deployment group name.");
    }

    final DeploymentGroup deploymentGroup = client.deploymentGroup(name).get();

    if (deploymentGroup == null) {
      out.printf("Unknown deployment group: %s%n", name);
      return 1;
    }

    if (json) {
      out.println(Json.asPrettyStringUnchecked(deploymentGroup));
    } else {
      out.printf("Name: %s%n", deploymentGroup.getName());
      printMap(out, "Labels: ", deploymentGroup.getLabels());
      out.printf("Job: %s%n", deploymentGroup.getJob());
    }

    return 0;
  }

  private <K extends Comparable<K>, V> void printMap(final PrintStream out, final String name,
                                                     final Map<K, V> values) {
    out.print(name);
    boolean first = true;
    for (final K key : Ordering.natural().sortedCopy(values.keySet())) {
      if (!first) {
        out.print(Strings.repeat(" ", name.length()));
      }
      final V value = values.get(key);
      out.printf("%s=%s%n", key, value);
      first = false;
    }
    if (first) {
      out.println();
    }
  }
}

