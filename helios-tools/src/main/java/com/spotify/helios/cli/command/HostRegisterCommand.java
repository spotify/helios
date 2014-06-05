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

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class HostRegisterCommand extends ControlCommand {

  private final Argument hostArg;
  private final Argument idArg;

  public HostRegisterCommand(final Subparser parser) {
    super(parser);

    parser.help("register a host");

    hostArg = parser.addArgument("host");
    idArg = parser.addArgument("id");

  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, final boolean json)
      throws ExecutionException, InterruptedException {
    final String host = options.getString(hostArg.getDest());
    final String id = options.getString(idArg.getDest());

    out.printf("Registering host %s with id %s%n", host, id);

    int code = 0;
    out.printf("%s: ", host);
    final int result = client.registerHost(host, id).get();
    if (result == 200) {
      out.printf("done%n");
    } else {
      out.printf("failed: %s%n", result);
      code = 1;
    }

    return code;
  }
}
