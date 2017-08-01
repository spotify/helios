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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.Response;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class DeploymentGroupStopCommand extends ControlCommand {

  private final Argument nameArg;

  public DeploymentGroupStopCommand(final Subparser parser) {
    super(parser);

    parser.help("Stop a deployment-group or abort a rolling-update");

    nameArg = parser.addArgument("name")
        .required(true)
        .help("Deployment group name");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());

    final int status = client.stopDeploymentGroup(name).get();

    // TODO(staffam): Support json output

    if (status == Response.Status.NO_CONTENT.getStatusCode()) {
      out.println(format("Deployment-group %s stopped", name));
      return 0;
    } else if (status == Response.Status.NOT_FOUND.getStatusCode()) {
      out.println(format("Deployment-group %s not found", name));
      return 1;
    } else {
      out.println(format("Failed to stop deployment-group %s. Status: %d",
          name, status));
      return 1;
    }
  }
}

