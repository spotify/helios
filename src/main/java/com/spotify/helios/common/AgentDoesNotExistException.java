package com.spotify.helios.common;

public class AgentDoesNotExistException extends HeliosException {

  public AgentDoesNotExistException(final String message) {
    super(message);
  }

  public AgentDoesNotExistException(final Throwable cause) {
    super(cause);
  }

  public AgentDoesNotExistException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
