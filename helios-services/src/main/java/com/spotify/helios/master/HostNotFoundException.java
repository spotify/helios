package com.spotify.helios.master;

import com.spotify.helios.common.HeliosException;

public class HostNotFoundException extends HeliosException {

  public HostNotFoundException(final String message) {
    super(message);
  }

  public HostNotFoundException(final Throwable cause) {
    super(cause);
  }

  public HostNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
