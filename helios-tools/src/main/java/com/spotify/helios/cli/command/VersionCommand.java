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
import com.spotify.helios.common.protocol.VersionResponse;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class VersionCommand extends ControlCommand {

  public VersionCommand(Subparser parser) {
    super(parser);
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {

    final VersionResponse response = client.version().get();

    if (json) {
      out.println(Json.asPrettyStringUnchecked(response));
    } else {
      out.println(String.format("Client Version: %s%nMaster Version: %s",
          response.getClientVersion(), response.getMasterVersion()));
    }
    return 0;
  }

}
