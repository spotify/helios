/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;


import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

import static java.lang.String.format;

public class JobDoesNotExistException extends HeliosException {

  public JobDoesNotExistException(final String message) {
    super(message);
  }

  public JobDoesNotExistException(final Throwable cause) {
    super(cause);
  }

  public JobDoesNotExistException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public JobDoesNotExistException(final JobId id) {
    super(format("job does not exist: %s", id));
  }
}
