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

package com.spotify.helios.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;

import java.util.Map;

public class Execution {

  private final Job job;
  private final Map<String, Integer> ports;
  private final Goal goal;

  public Execution(@JsonProperty("job") final Job job,
                   @JsonProperty("ports") final Map<String, Integer> ports,
                   @JsonProperty("goal") final Goal goal) {
    this.job = job;
    this.ports = ports;
    this.goal = goal;
  }

  public Job getJob() {
    return job;
  }

  public Map<String, Integer> getPorts() {
    return ports;
  }

  public Goal getGoal() {
    return goal;
  }

  public Execution withGoal(final Goal goal) {
    return new Execution(job, ports, goal);
  }

  public Execution withPorts(final Map<String, Integer> ports) {
    return new Execution(job, ports, goal);
  }

  public static Execution of(final Job job) {
    return new Execution(job, null, null);
  }

  @Override
  public String toString() {
    return "Execution{" +
           "job=" + job +
           ", ports=" + ports +
           ", goal=" + goal +
           '}';
  }
}
