/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.spotify.helios.common.descriptors.JobId;

import static java.lang.String.format;

public class JobPortAllocationConflictException extends HeliosException {

  private final JobId id;
  private final JobId conflictingId;
  private final String agent;
  private final int port;

  public JobPortAllocationConflictException(final JobId id, final JobId conflictingId,
                                            final String agent, final int port) {

    super(format("Allocation of port %d for job %s collides with job %s on host %s",
                 port, id, conflictingId, agent));
    this.id = id;
    this.conflictingId = conflictingId;
    this.agent = agent;
    this.port = port;
  }

  public JobId getId() {
    return id;
  }

  public JobId getConflictingId() {
    return conflictingId;
  }

  public String getAgent() {
    return agent;
  }

  public int getPort() {
    return port;
  }
}
