/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.common;

public class HeliosException extends Exception {

  public HeliosException(final String message) {
    super(message);
  }

  public HeliosException(final Throwable cause) {
    super(cause);
  }

  public HeliosException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
