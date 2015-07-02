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

import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class DeploymentGroupCreateCommand extends ControlCommand {

  private final Argument nameArg;
  private final Argument labelsArg;
  private final Argument quietArg;

  public DeploymentGroupCreateCommand(final Subparser parser) {
    super(parser);

    parser.help("create a deployment group");

    nameArg = parser.addArgument("name")
        .nargs("?")
        .help("Deployment group name");

    labelsArg = parser.addArgument("labels")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Only include hosts that match all of these labels. Separate multiple labels with "
              + "spaces, e.g. 'foo=bar baz=qux'.");

    quietArg = parser.addArgument("-q")
        .action(storeTrue())
        .help("only print job id");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final DeploymentGroup.Builder builder;

    final String name = options.getString(nameArg.getDest());
    final Map<String, String> labels = Utils.argToStringMap(options, labelsArg);
    final boolean quiet = options.getBoolean(quietArg.getDest());

    if (name == null || labels.isEmpty()) {
      throw new IllegalArgumentException("Please specify a name and at least one label.");
    }

    builder = DeploymentGroup.newBuilder().setName(name).setLabels(labels);
    final DeploymentGroup deploymentGroup = builder.build();

    if (!quiet && !json) {
      out.println("Creating deployment group: " + deploymentGroup.toJsonString());
    }

    final CreateDeploymentGroupResponse status =
        client.createDeploymentGroup(deploymentGroup).get();

    if (status == null) {
      throw new RuntimeException("The Helios master could not create a deployment group.");
    }

    if (status.getStatus() != CreateDeploymentGroupResponse.Status.CONFLICT) {
      out.println(status.toJsonString());
      return 0;
    } else {
      if (!quiet && !json) {
        out.println("Failed: " + status);
      } else if (json) {
        out.println(status.toJsonString());
      }
      return 1;
    }
  }
}

