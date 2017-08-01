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

import static java.lang.String.format;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class DeploymentGroupRemoveCommand extends ControlCommand {

  private final Argument nameArg;

  public DeploymentGroupRemoveCommand(final Subparser parser) {
    super(parser);

    parser.help("remove a deployment-group. Note that this does not undeploy jobs previously "
                + "deployed by the deployment-group");

    nameArg = parser.addArgument("deployment-group-name")
        .required(true)
        .help("Deployment group name");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());

    final RemoveDeploymentGroupResponse status = client.removeDeploymentGroup(name).get();

    if (status == null) {
      throw new RuntimeException("The Helios master could not remove the given deployment group.");
    }

    final boolean failed = status.getStatus() != RemoveDeploymentGroupResponse.Status.REMOVED;

    if (json) {
      out.println(status.toJsonString());
    } else {
      if (failed) {
        out.println(format("Failed to remove deployment-group %s, status: %s",
            name, status.getStatus()));
      } else {
        out.println(format("Deployment-group %s removed", name));
      }
    }

    return failed ? 1 : 0;
  }
}

