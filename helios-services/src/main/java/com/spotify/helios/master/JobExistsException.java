/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;

public class JobExistsException extends HeliosException {

  public JobExistsException(final String message) {
    super(message);
  }

  public JobExistsException(final Throwable cause) {
    super(cause);
  }

  public JobExistsException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
