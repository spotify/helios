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

import com.google.common.base.Strings;

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
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class DeploymentGroupWatchCommand extends ControlCommand {

  private static final int MAX_WIDTH = 80;
  private static final String DATE_TIME_PATTERN = "YYYY-MM-dd HH:mm:ss";

  private final Argument nameArg;
  private final Argument fullArg;
  private final Argument intervalArg;

  public DeploymentGroupWatchCommand(Subparser parser) {
    super(parser);
    parser.help("watch deployment groups");

    nameArg = parser.addArgument("name")
        .help("Deployment group name");

    fullArg = parser.addArgument("-f")
        .action(storeTrue())
        .help("Print full hostnames and job ids.");

    intervalArg = parser.addArgument("--interval")
        .type(Integer.class)
        .setDefault(1)
        .help("polling interval, default 1 second");
  }

  @Override
  int run(Namespace options, HeliosClient client, PrintStream out, boolean json,
          BufferedReader stdin) throws ExecutionException, InterruptedException, IOException {
    final String name = options.getString(nameArg.getDest());
    final boolean full = options.getBoolean(fullArg.getDest());
    final Integer interval = options.getInt(intervalArg.getDest());
    final DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_TIME_PATTERN);

    if (!json) {
      out.println("Control-C to stop");
      out.println("STATUS               HOST                           STATE");
    }

    final int timestampLength = String.format("[%s UTC]", DATE_TIME_PATTERN).length();

    int rc = 0;
    while (rc == 0) {
      final Instant now = new Instant();
      if (!json) {
        out.printf(Strings.repeat("-", MAX_WIDTH - timestampLength - 1)
                   + " [%s UTC]%n", now.toString(formatter));
      }

      rc = DeploymentGroupStatusCommand.run0(client, out, json, name, full);
      if (out.checkError()) {
        break;
      }

      Thread.sleep(1000 * interval);
    }
    return 0;
  }
}
