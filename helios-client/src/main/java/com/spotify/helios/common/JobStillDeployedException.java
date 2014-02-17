package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

import java.util.List;

public class JobStillDeployedException extends HeliosException {

  private final JobId id;
  private final List<String> hosts;

  public JobStillDeployedException(JobId id, List<String> hosts) {
    super(String.format("job %s still deployed on hosts %s", id, hosts));
    this.id = id;
    this.hosts = hosts;
  }

  public JobId getId() {
    return id;
  }

  public List<String> getHosts() {
    return hosts;
  }
}
