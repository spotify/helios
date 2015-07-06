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

package com.spotify.helios.cli;

import java.io.PrintStream;
import java.util.List;

import static com.spotify.helios.cli.Output.formatHostname;
import static com.spotify.helios.cli.Output.table;

public class DeploymentGroupStatusTable {

  private final Table table;

  public DeploymentGroupStatusTable(final PrintStream out, final List<String> hosts,
                                    final int hostIndex, final boolean full) {
    this.table = table(out);
    table.row("HOST", "DONE");


    int i = 0;
    for (final String host : hosts) {
      final String displayHostName = formatHostname(full, host);

      if (i < hostIndex) {
        table.row(displayHostName, "X");
      } else {
        // The deployment hasn't happened yet.
        table.row(displayHostName, "");
      }

      i++;
    }
  }

  public void print() {
    table.print();
  }
}
