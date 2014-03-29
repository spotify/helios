/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

import static java.lang.String.format;

public class JobPortAllocationConflictException extends HeliosException {

  private final JobId id;
  private final JobId conflictingId;
  private final String host;
  private final int port;

  public JobPortAllocationConflictException(final JobId id, final JobId conflictingId,
                                            final String host, final int port) {

    super(format("Allocation of port %d for job %s collides with job %s on host %s",
                 port, id, conflictingId, host));
    this.id = id;
    this.conflictingId = conflictingId;
    this.host = host;
    this.port = port;
  }

  public JobId getId() {
    return id;
  }

  public JobId getConflictingId() {
    return conflictingId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}
