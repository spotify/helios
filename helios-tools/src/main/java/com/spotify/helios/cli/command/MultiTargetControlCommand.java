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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.spotify.helios.cli.Target;
import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Abstract class for commands that need to operate on multiple domains in parallel.
 * This is in contrast to a normal {@link ControlCommand}, which can operate on multiple
 * domains but does so sequentially.
 */
public abstract class MultiTargetControlCommand implements CliCommand {
  MultiTargetControlCommand(final Subparser parser) {
    parser.setDefault("command", this).defaultHelp(true);
  }

  @Override
  public int run(final Namespace options, final List<Target> targets, final PrintStream out,
                 final PrintStream err, final String username, final boolean json,
                 final Path authPlugin, final Path privateKeyPath, final BufferedReader stdin)
      throws IOException, InterruptedException {

    final Builder<TargetAndClient> clientBuilder = ImmutableList.builder();
    for (final Target target : targets) {
      final HeliosClient client =
          Utils.getClient(target, err, username, authPlugin, privateKeyPath);
      if (client == null) {
        return 1;
      }
      clientBuilder.add(new TargetAndClient(target, client));
    }

    final List<TargetAndClient> clients = clientBuilder.build();

    final int result;
    try {
      result = run(options, clients, out, json, stdin);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      // if target is a site, print message like
      // "Request timed out to master in ash.spotify.net (http://ash2-helios-a4.ash2.spotify.net)",
      // otherwise "Request timed out to master http://ash2-helios-a4.ash2.spotify.net:5800"
      if (cause instanceof TimeoutException) {
        err.println("Request timed out to master");
      } else {
        throw Throwables.propagate(cause);
      }
      return 1;
    } finally {
      for (TargetAndClient cc : clients) {
        cc.getClient().close();
      }
    }
    return result;
  }

  abstract int run(final Namespace options, final List<TargetAndClient> clients,
                   final PrintStream out, final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException;
}
