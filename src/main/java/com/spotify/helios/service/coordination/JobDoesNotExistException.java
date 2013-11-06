/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.coordination;

import com.spotify.helios.common.HeliosException;

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
}
