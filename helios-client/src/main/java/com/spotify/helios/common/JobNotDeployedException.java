package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

public class JobNotDeployedException extends HeliosException {
  private final String agent;
  private final JobId jobId;

  public JobNotDeployedException(String agent, JobId jobId, Exception e) {
    super(String.format("Job [%s] is not deployed on host [%s]", jobId, agent), e);
    this.agent = agent;
    this.jobId = jobId;
  }

  public String getAgent() {
    return agent;
  }

  public JobId getJobId() {
    return jobId;
  }
}
