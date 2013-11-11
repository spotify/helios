package com.spotify.helios.service.coordination;

import com.spotify.helios.common.HeliosException;

public class JobAlreadyDeployedException extends HeliosException {
  public JobAlreadyDeployedException(String agent, String job) {
    super(String.format("Job [%s] already deployed on agent [%s]", agent, job));
  }
}
