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

import com.google.common.collect.Sets;

import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static com.spotify.helios.cli.Output.formatHostname;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class MasterListCommand extends ControlCommand {

  private final Argument fullArg;

  public MasterListCommand(Subparser parser) {
    super(parser);
    parser.help("list masters");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full hostnames");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException, HeliosAuthException {

    final List<String> masters = client.listMasters().get();
    final boolean full = options.getBoolean(fullArg.getDest());

    final SortedSet<String> sortedMasters = Sets.newTreeSet();

    if (json) {
      for (final String host : masters) {
        sortedMasters.add(formatHostname(full, host));
      }
      out.println(Json.asPrettyStringUnchecked(sortedMasters));
    } else {
      for (final String host : masters) {
        out.println(formatHostname(full, host));
      }
    }

    return 0;
  }
}
