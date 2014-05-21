package com.spotify.helios.agent.docker;

import java.net.URI;

public class DockerTimeoutException extends DockerException {

  private final String method;
  private final URI uri;

  public DockerTimeoutException(final String method, final URI uri, final Throwable cause) {
    super("Timeout: " + method + " " + uri, cause);
    this.method = method;
    this.uri = uri;
  }

  public String method() {
    return method;
  }

  public URI uri() {
    return uri;
  }
}
