package com.spotify.helios.agent;

import com.kpelykh.docker.client.DockerClient;
import com.sun.jersey.api.client.ClientResponse;

public class PullClientResponse {
  private final ClientResponse response;
  private final DockerClient client;

  public PullClientResponse(ClientResponse response, DockerClient client) {
    this.response = response;
    this.client = client;
  }

  public ClientResponse getResponse() {
    return response;
  }

  public void close() {
    client.closeConnection();
  }
}
