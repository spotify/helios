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

import com.spotify.helios.cli.Target;
import com.spotify.helios.cli.Utils;
import com.spotify.helios.client.HeliosClient;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Strings.repeat;
import static java.lang.String.format;

public abstract class ControlCommand implements CliCommand {

  public static final int BATCH_SIZE = 10;
  public static final int QUEUE_SIZE = 1000;

  ControlCommand(final Subparser parser) {

    parser.setDefault("command", this).defaultHelp(true);
  }

  @Override
  public int run(final Namespace options, final List<Target> targets, final PrintStream out,
                 final PrintStream err, final String username, final boolean json,
                 final BufferedReader stdin)
      throws IOException, InterruptedException {
    boolean successful = true;

    boolean isFirst = true;

    // TODO (dxia) The json = true branches below are a hack to get valid JSON for multiple domains.
    // Refactor concrete command implementations later to return an Object that can then be put into
    // a {"$DOMAIN": $VALUE} dict before json serializing and returning it.

    // Execute the control command over each target cluster
    Iterator<Target> targetIterator = targets.iterator();
    while (targetIterator.hasNext()) {
      Target target = targetIterator.next();

      if (targets.size() > 1) {
        if (!json) {
          final List<URI> endpoints = target.getEndpointSupplier().get();
          final String header = format("%s (%s)", target.getName(), endpoints);
          out.println(header);
          out.println(repeat("-", header.length()));
          out.flush();
        } else {
          if (isFirst) {
            out.println("{");
            isFirst = false;
          }
          out.println("\"" + target.getName() + "\": ");
        }
      }

      successful &= run(options, target, out, err, username, json, stdin);

      if (targets.size() > 1) {
        if (!json) {
          out.println();
        } else {
          if (targetIterator.hasNext()) {
            out.println(",\n");
          } else {
            out.println("}");
          }
        }
      }
    }

    return successful ? 0 : 1;
  }

  /**
   * Execute against a cluster at a specific endpoint
   * @param stdin TODO
   */
  private boolean run(final Namespace options, final Target target, final PrintStream out,
                      final PrintStream err, final String username, final boolean json,
                      final BufferedReader stdin)
      throws InterruptedException, IOException {

    final HeliosClient client = Utils.getClient(target, err, username);
    if (client == null) {
      return false;
    }

    try {
      final int result = run(options, client, out, json, stdin);
      return result == 0;
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      // if target is a domain, print message like
      // "Request timed out to master in ash.spotify.net (http://ash2-helios-a4.ash2.spotify.net)",
      // otherwise "Request timed out to master http://ash2-helios-a4.ash2.spotify.net:5800"
      if (cause instanceof TimeoutException) {
        err.println("Request timed out to master in " + target);
      } else {
        throw Throwables.propagate(cause);
      }
      return false;
    } finally {
      client.close();
    }
  }

  abstract int run(final Namespace options, final HeliosClient client, PrintStream out,
                   final boolean json, BufferedReader stdin)
      throws ExecutionException, InterruptedException, IOException;
}
