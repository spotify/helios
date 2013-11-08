/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AgentJobDescriptor extends Descriptor {

  private final AgentJob job;
  private final JobDescriptor descriptor;

  public AgentJobDescriptor(@JsonProperty("job") final AgentJob job,
                            @JsonProperty("descriptor") final JobDescriptor descriptor) {
    this.job = job;
    this.descriptor = descriptor;
  }

  public AgentJob getJob() {
    return job;
  }

  public JobDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AgentJobDescriptor that = (AgentJobDescriptor) o;

    if (descriptor != null ? !descriptor.equals(that.descriptor) : that.descriptor != null) {
      return false;
    }
    if (job != null ? !job.equals(that.job) : that.job != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (descriptor != null ? descriptor.hashCode() : 0);
    return result;
  }
}
