package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

import java.util.List;

import static java.lang.String.format;

public class HostStillInUseException extends HeliosException {

  private final String host;
  private final List<JobId> jobs;

  public HostStillInUseException(final String host, final List<JobId> jobs) {
    super(format("Jobs still deployed on host %s: %s", host, jobs));
    this.host = host;
    this.jobs = jobs;
  }

  public String getHost() {
    return host;
  }

  public List<JobId> getJobs() {
    return jobs;
  }
}
