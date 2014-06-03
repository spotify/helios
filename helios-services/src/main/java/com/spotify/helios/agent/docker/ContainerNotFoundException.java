package com.spotify.helios.agent.docker;

public class ContainerNotFoundException extends DockerException {

  private final String containerId;

  public ContainerNotFoundException(final String containerId, final Throwable cause) {
    super("Container not found: " + containerId, cause);
    this.containerId = containerId;
  }

  public ContainerNotFoundException(final String containerId) {
    this(containerId, null);
  }

  public String getContainerId() {
    return containerId;
  }
}
