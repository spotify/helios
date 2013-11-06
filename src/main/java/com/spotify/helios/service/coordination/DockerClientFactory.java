/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.coordination;

import com.kpelykh.docker.client.DockerClient;

public class DockerClientFactory {
  private final String endpoint;

  public DockerClientFactory(final String endpoint) {
    this.endpoint = endpoint;
  }

  public DockerClient create() {
    return new DockerClient(endpoint);
  }
}
