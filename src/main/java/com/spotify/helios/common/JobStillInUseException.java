package com.spotify.helios.common;

public class JobStillInUseException extends HeliosException {

  private final String id;
  private final String agent;

  public JobStillInUseException(String id, String agent) {
    super(String.format("job id %s still in use by host %s", id, agent));
    this.id = id;
    this.agent = agent;
  }

  public String getId() {
    return id;
  }

  public String getAgent() {
    return agent;
  }
}
