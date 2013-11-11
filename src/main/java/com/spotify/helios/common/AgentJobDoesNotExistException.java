package com.spotify.helios.common;

public class AgentJobDoesNotExistException extends HeliosException {
  private final String agent;
  private final String job;

  public AgentJobDoesNotExistException(String agent, String job, Exception e) {
    super(String.format("Job [%s] does not exist on host [%s]", job, agent), e);
    this.agent = agent;
    this.job = job;
  }

  public String getAgent() {
    return agent;
  }

  public String getJob() {
    return job;
  }
}
