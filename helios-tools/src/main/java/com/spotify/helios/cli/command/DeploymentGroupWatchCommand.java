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

import com.google.common.base.Optional;

import com.spotify.helios.cli.Target;
import com.spotify.helios.client.HeliosClient;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;

public class DeploymentGroupWatchCommand extends MultiTargetControlCommand {

  private final Argument nameArg;
  private final Argument intervalArg;

  public DeploymentGroupWatchCommand(Subparser parser) {
    super(parser);
    parser.help("watch deployment groups");

    nameArg = parser.addArgument("name")
        .help("Deployment group name");

    intervalArg = parser.addArgument("--interval")
        .type(Integer.class)
        .setDefault(1)
        .help("polling interval, default 1 second");
  }

  @Override
  int run(final Namespace options, final List<TargetAndClient> clients,
          final PrintStream out, final boolean json, final BufferedReader stdin)
              throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());

    watchDeploymentGroup(out, name, options.getInt(intervalArg.getDest()), clients);
    return 0;
  }

  static void watchDeploymentGroup(final PrintStream out, final String name,
                                   final int interval, final List<TargetAndClient> clients)
      throws InterruptedException, ExecutionException {
    out.println("Control-C to stop");
    out.println("STATUS               HOST                           STATE");
    final DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
    while (true) {

      final Instant now = new Instant();
      out.printf("-------------------- ------------------------------ -------- "
          + "---------- [%s UTC]%n", now.toString(formatter));
      for (TargetAndClient cc : clients) {
        final Optional<Target> target = cc.getTarget();
        if (clients.size() > 1) {
          final String header;
          if (target.isPresent()) {
            final List<URI> endpoints = target.get().getEndpointSupplier().get();
            header = format(" %s (%s)", target.get().getName(), endpoints);
          } else {
            header = "";
          }
          out.printf("---%s%n", header);
        }
        showReport(out, name, cc.getClient());
      }
      if (out.checkError()) {
        break;
      }
      Thread.sleep(1000 * interval);
    }
  }

  private static void showReport(PrintStream out, final String name, final HeliosClient client)
      throws ExecutionException, InterruptedException {
    out.printf("%s%n", name);
  }
}
