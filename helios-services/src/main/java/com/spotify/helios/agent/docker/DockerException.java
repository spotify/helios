package com.spotify.helios.agent.docker;

public class DockerException extends Exception {

  public DockerException(final String message) {
    super(message);
  }

  public DockerException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public DockerException(final Throwable cause) {
    super(cause);
  }
}
