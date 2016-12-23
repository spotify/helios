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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class DeploymentGroupListCommand extends ControlCommand {

  public DeploymentGroupListCommand(final Subparser parser) {
    super(parser);

    parser.help("list deployment groups");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {

    final List<String> deploymentGroups = client.listDeploymentGroups().get();

    if (deploymentGroups != null) {
      if (json) {
        out.println(Json.asPrettyStringUnchecked(deploymentGroups));
      } else {
        for (final String deploymentGroup : deploymentGroups) {
          out.println(deploymentGroup);
        }
      }
      return 0;
    } else {
      out.println("Failed to list deployment groups");
      return 1;
    }
  }
}

