/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.coordination;

public class CuratorException extends Exception {

  public CuratorException() {
  }

  public CuratorException(final String message) {
    super(message);
  }

  public CuratorException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public CuratorException(final Throwable cause) {
    super(cause);
  }

  public CuratorException(final String message, final Throwable cause,
                          final boolean enableSuppression,
                          final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
