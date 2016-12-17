/*-
 * -\-\-
 * Helios Service Registration
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.serviceregistration;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;

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

    @Deprecated
    public Builder endpoint(final String name,
                            final String protocol,
                            final int port) {
      endpoints.add(new Endpoint(name, protocol, port, "", "", null, null));
      return this;
    }

    public Builder endpoint(final String name,
                            final String protocol,
                            final int port,
                            final String domain,
                            final String host) {
      endpoints.add(new Endpoint(name, protocol, port, domain, host, null, null));
      return this;
    }

    public Builder endpoint(final String name,
                            final String protocol,
                            final int port,
                            final String domain,
                            final String host,
                            final List<String> tags) {
      endpoints.add(new Endpoint(name, protocol, port, domain, host, tags, null));
      return this;
    }

    public Builder endpoint(final String name,
                            final String protocol,
                            final int port,
                            final String domain,
                            final String host,
                            final List<String> tags,
                            final EndpointHealthCheck healthCheck) {
      endpoints.add(new Endpoint(name, protocol, port, domain, host, tags, healthCheck));
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
    private final String domain;

    // The hostname on which we will advertise this service in service discovery.
    private final String host;
    private final List<String> tags;
    private final EndpointHealthCheck healthCheck;

    public Endpoint(final String name, final String protocol, final int port,
                    final String domain, final String host, final List<String> tags,
                    final EndpointHealthCheck healthCheck) {
      this.name = name;
      this.protocol = protocol;
      this.port = port;
      this.domain = domain;
      this.host = host;
      this.tags = tags;
      this.healthCheck = healthCheck;
    }

    public String getHost() {
      return host;
    }

    public String getDomain() {
      return domain;
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

    public List<String> getTags() {
      return tags;
    }

    public EndpointHealthCheck getHealthCheck() {
      return healthCheck;
    }

    @Override
    public String toString() {
      return "Endpoint{"
             + "name='" + name + '\''
             + ", protocol='" + protocol + '\''
             + ", port=" + port
             + ", domain='" + domain + '\''
             + ", host='" + host + '\''
             + ", tags=" + tags
             + ", healthCheck=" + healthCheck
             + '}';
    }
  }

  public static class EndpointHealthCheck {
    public static final String HTTP = "http";
    public static final String TCP = "tcp";

    private final String type;
    private final String path;

    public EndpointHealthCheck(final String type, final String path) {
      this.type = type;
      this.path = path;
    }

    public String getType() {
      return type;
    }

    public String getPath() {
      return path;
    }

    public static EndpointHealthCheck newHttpCheck(String path) {
      return new EndpointHealthCheck(HTTP, path);
    }

    public static EndpointHealthCheck newTcpCheck() {
      return new EndpointHealthCheck(TCP, null);
    }

    @Override
    public String toString() {
      return "EndpointHealthCheck{"
             + "type='" + type + '\''
             + ", path='" + path + '\''
             + '}';
    }
  }
}
