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

package com.spotify.helios.cli;

import static com.google.common.base.Ascii.truncate;
import static com.google.common.base.Optional.fromNullable;
import static com.spotify.helios.cli.Output.table;

import com.google.common.base.Joiner;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JobStatusTable {

  private final Table table;
  private final boolean full;

  public JobStatusTable(final PrintStream out, final boolean full) {
    this.table = table(out);
    this.full = full;
    table.row("JOB ID", "HOST", "GOAL", "STATE", "CONTAINER ID", "PORTS");
  }

  public void task(final JobId jobId, final String host, final TaskStatus ts,
                   final Deployment deployment) {
    final String goal = (deployment == null) ? "" : deployment.getGoal().toString();
    final int maxContainerId = full ? Integer.MAX_VALUE : 7;
    final String jobIdString = full ? jobId.toString() : jobId.toShortString();
    if (ts == null) {
      table.row(jobIdString, host, goal, "", "", "");
    } else {
      final List<String> portMappings = new ArrayList<>();
      for (final Map.Entry<String, PortMapping> entry : ts.getPorts().entrySet()) {
        final PortMapping portMapping = entry.getValue();
        portMappings.add(String.format("%s=%d:%d", entry.getKey(),
            portMapping.getInternalPort(),
            portMapping.getExternalPort()));
      }
      String state = ts.getState().toString();
      if (ts.getThrottled() != ThrottleState.NO) {
        state += " (" + ts.getThrottled() + ")";
      }
      final String ports = Joiner.on(" ").join(portMappings);
      final String cid = truncate(fromNullable(ts.getContainerId()).or(""), maxContainerId, "");
      table.row(jobIdString, host, goal, state, cid, ports);
    }
  }

  public void print() {
    table.print();
  }
}
