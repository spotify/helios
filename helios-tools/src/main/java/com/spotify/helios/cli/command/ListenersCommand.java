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
import com.spotify.helios.common.Json;
import com.spotify.helios.common.protocol.ListenerResponse;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

public class ListenersCommand extends ControlCommand {

  private final Argument listenerUrl;

  public ListenersCommand ( Subparser parser ) {
    super(parser);

    parser.help("register event listeners");

    listenerUrl = parser.addArgument("listener").
                         type(String.class).
                         help("Listener URL");
  }

  @Override
  int run(final Namespace options, final HeliosClient client, final PrintStream out,
          final boolean json, final BufferedReader stdin)
      throws ExecutionException, InterruptedException {

    String url = options.getString(listenerUrl.getDest());
    ListenerResponse result = client.listeners(url).get();

    if (json) {
      out.println(Json.asPrettyStringUnchecked(result));
    }
    
    return 0;
  }
}
