/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

public class HeliosRuntimeException extends RuntimeException {

  public HeliosRuntimeException() {
  }

  public HeliosRuntimeException(final String message) {
    super(message);
  }

  public HeliosRuntimeException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public HeliosRuntimeException(final Throwable cause) {
    super(cause);
  }

  public HeliosRuntimeException(final String message, final Throwable cause,
                                final boolean enableSuppression,
                                final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
