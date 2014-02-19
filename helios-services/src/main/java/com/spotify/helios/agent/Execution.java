/**
 * Copyright (C) 2014 Spotify AB
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
}
