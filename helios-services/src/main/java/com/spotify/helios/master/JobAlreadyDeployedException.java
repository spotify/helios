package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

public class JobAlreadyDeployedException extends HeliosException {

  public JobAlreadyDeployedException(String host, JobId job) {
    super(String.format("Job [%s] already deployed on host [%s]", host, job));
  }
}
