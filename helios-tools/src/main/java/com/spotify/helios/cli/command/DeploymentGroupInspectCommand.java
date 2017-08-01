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

import com.google.common.collect.Maps;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.RolloutOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class DeploymentGroupInspectCommand extends ControlCommand {

  private final Argument nameArg;

  public DeploymentGroupInspectCommand(final Subparser parser) {
    super(parser);

    parser.help("inspect a deployment group");

    nameArg = parser.addArgument("name")
        .required(true)
        .help("Deployment group name");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());

    final DeploymentGroup deploymentGroup = client.deploymentGroup(name).get();

    if (deploymentGroup == null) {
      if (json) {
        final Map<String, Object> output = Maps.newHashMap();
        output.put("status", "DEPLOYMENT_GROUP_NOT_FOUND");
        out.print(Json.asStringUnchecked(output));
      } else {
        out.printf("Unknown deployment group: %s%n", name);
      }
      return 1;
    }

    if (json) {
      out.println(Json.asPrettyStringUnchecked(deploymentGroup));
    } else {
      out.printf("Name: %s%n", deploymentGroup.getName());
      out.printf("Host selectors:%n");
      for (final HostSelector hostSelector : deploymentGroup.getHostSelectors()) {
        out.printf("  %s%n", hostSelector.toPrettyString());
      }
      out.printf("Job: %s%n", deploymentGroup.getJobId());

      if (deploymentGroup.getRollingUpdateReason() != null) {
        out.printf("Rolling update reason: %s%n", deploymentGroup.getRollingUpdateReason());
      }

      final RolloutOptions rolloutOptions = deploymentGroup.getRolloutOptions();
      if (rolloutOptions != null) {
        out.printf("Rollout options:%n");
        out.printf("  Migrate: %s%n", rolloutOptions.getMigrate());
        out.printf("  Overlap: %s%n", rolloutOptions.getOverlap());
        out.printf("  Parallelism: %d%n", rolloutOptions.getParallelism());
        out.printf("  Timeout: %d%n", rolloutOptions.getTimeout());
        if (!isNullOrEmpty(rolloutOptions.getToken())) {
          out.printf("  Token: %s%n", rolloutOptions.getToken());
        }
        out.printf("  Ignore failures: %b%n", rolloutOptions.getIgnoreFailures());
      }

    }

    return 0;
  }
}

