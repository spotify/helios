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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.protocol.HostDeregisterResponse;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class HostDeregisterCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument forceArg;

  public HostDeregisterCommand(Subparser parser) {
    super(parser);

    parser.help("deregister a host");

    hostArg = parser.addArgument("host")
        .help("Host name to deregister.");

    forceArg = parser.addArgument("--force")
        .action(Arguments.storeTrue())
        .help("Force deregistration.");
  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException, IOException {
    final String host = options.getString(hostArg.getDest());
    final boolean force = options.getBoolean(forceArg.getDest());

    if (!force) {
      out.printf("This will deregister the host %s%n", host);
      out.printf("Do you want to continue? [y/N]%n");

      // TODO (dano): pass in stdin instead using System.in
      final int c = System.in.read();

      if (c != 'Y' && c != 'y') {
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
      code = 1;
    }
    return code;
  }
}
