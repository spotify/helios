package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

public class JobNotDeployedException extends HeliosException {

  private final String host;
  private final JobId jobId;

  public JobNotDeployedException(String host, JobId jobId) {
    super(String.format("Job [%s] is not deployed on host [%s]", jobId, host));
    this.host = host;
    this.jobId = jobId;
  }

  public String getHost() {
    return host;
  }

  public JobId getJobId() {
    return jobId;
  }
}
