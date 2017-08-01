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

import com.google.common.base.Joiner;
import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostDeregisterCommand extends ControlCommand {

  private static final Logger log = LoggerFactory.getLogger(HostDeregisterCommand.class);

  private final Argument hostArg;
  private final Argument yesArg;
  private final Argument forceArg;

  public HostDeregisterCommand(Subparser parser) {
    super(parser);

    parser.help("deregister a host");

    hostArg = parser.addArgument("host")
        .help("Host name to deregister.");

    yesArg = parser.addArgument("--yes")
        .action(Arguments.storeTrue())
        .help("Automatically answer 'yes' to the interactive prompt.");

    // TODO (dxia) Deprecated, remove at a later date
    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Automatically answer 'yes' to the interactive prompt.");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException {
    final String host = options.getString(hostArg.getDest());
    final boolean yes = options.getBoolean(yesArg.getDest());
    final boolean force = options.getBoolean(forceArg.getDest());

    if (force) {
      log.warn("If you are using '--force' to skip the interactive prompt, "
               + "note that we have deprecated it. Please use '--yes'.");
    }

    if (!yes && !force) {
      out.printf("This will deregister the host %s%n", host);
      final boolean confirmed = Utils.userConfirmed(out, stdin);
      if (!confirmed) {
        return 1;
      }
    }

    out.printf("Deregistering host %s%n", host);

    int code = 0;

    final HostDeregisterResponse response = client.deregisterHost(host).get();
    out.printf("%s: ", host);
    if (response.getStatus() == HostDeregisterResponse.Status.OK) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", response);

      if (response.getStatus() == HostDeregisterResponse.Status.NOT_FOUND) {
        final HostResolver resolver = HostResolver.create(client);
        final List<String> resolved = resolver.getSortedMatches(host);
        if (!resolved.isEmpty()) {
          out.println("We didn't find your hostname, but we did find some possible matches for you:"
                      + "\n    " + Joiner.on("\n    ").join(resolved) + "\n");
        }
      }
      code = 1;
    }
    return code;
  }
}
