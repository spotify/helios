package com.spotify.helios.agent.docker;

import java.net.URI;

public class DockerRequestException extends DockerException {

  private final String method;
  private final URI uri;
  private final int status;
  private final String message;

  public DockerRequestException(final String method, final URI uri, final Throwable cause) {
    this(method, uri, 0, null, cause);
  }

  public DockerRequestException(final String method, final URI uri,
                                final int status,
                                final Throwable cause) {
    this(method, uri, status, null, cause);
  }

  public DockerRequestException(final String method, final URI uri) {
    this(method, uri, 0, null, null);
  }

  public DockerRequestException(final String method, final URI uri,
                                final int status) {
    this(method, uri, status, null, null);
  }

  public DockerRequestException(final String method, final URI uri,
                                final int status, final String message) {
    this(method, uri, status, message, null);
  }

  public DockerRequestException(final String method, final URI uri,
                                final int status, final String message,
                                final Throwable cause) {
    super("Request error: " + method + " " + uri + ": " + status, cause);
    this.method = method;
    this.uri = uri;
    this.status = status;
    this.message = message;
  }

  public String method() {
    return method;
  }

  public URI uri() {
    return uri;
  }

  public int status() {
    return status;
  }

  public String message() {
    return message;
  }
}
