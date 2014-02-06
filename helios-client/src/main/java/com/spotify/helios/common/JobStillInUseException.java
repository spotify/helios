package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

import java.util.List;

public class JobStillInUseException extends HeliosException {

  private final JobId id;
  private final List<String> agents;

  public JobStillInUseException(JobId id, List<String> agents) {
    super(String.format("job id %s still in use by hosts %s", id, agents));
    this.id = id;
    this.agents = agents;
  }

  public JobId getId() {
    return id;
  }

  public List<String> getAgents() {
    return agents;
  }
}
