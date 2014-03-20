/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.serviceregistration;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * A list of endpoints to for service registration.
 */
public class ServiceRegistration {

  private final List<Endpoint> endpoints;

  public ServiceRegistration(List<Endpoint> endpoints) {
    this.endpoints = unmodifiableList(new ArrayList<>(endpoints));
  }

  public List<Endpoint> getEndpoints() {
    return endpoints;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private List<Endpoint> endpoints = new ArrayList<>();

    public Builder endpoint(final String name, final String protocol, final int port) {
      endpoints.add(new Endpoint(name, protocol, port));
      return this;
    }

    public ServiceRegistration build() {
      return new ServiceRegistration(endpoints);
    }
  }

  /**
   * A single service endpoint.
   */
  public static class Endpoint {

    private final String name;
    private final String protocol;
    private final int port;

    public Endpoint(final String name, final String protocol, final int port) {
      this.name = name;
      this.protocol = protocol;
      this.port = port;
    }

    public String getName() {
      return name;
    }

    public String getProtocol() {
      return protocol;
    }

    public int getPort() {
      return port;
    }
  }
}
