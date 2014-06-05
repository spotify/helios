package com.spotify.helios.agent;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;

import java.net.URI;

public class DockerClientFactory {

  private final URI dockerEndpoint;

  public DockerClientFactory(final String dockerEndpoint) {

    this.dockerEndpoint = URI.create(dockerEndpoint);
  }

  public DockerClient create() {
    return new DefaultDockerClient(dockerEndpoint);
  }
}
