package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

public class JobAlreadyDeployedException extends HeliosException {

  public JobAlreadyDeployedException(String agent, JobId job) {
    super(String.format("Job [%s] already deployed on agent [%s]", agent, job));
  }
}
