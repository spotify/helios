package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

public class JobStillInUseException extends HeliosException {

  private final JobId id;
  private final String agent;

  public JobStillInUseException(JobId id, String agent) {
    super(String.format("job id %s still in use by host %s", id, agent));
    this.id = id;
    this.agent = agent;
  }

  public JobId getId() {
    return id;
  }

  public String getAgent() {
    return agent;
  }
}
