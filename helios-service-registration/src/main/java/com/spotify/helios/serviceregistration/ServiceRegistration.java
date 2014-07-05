/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    @Deprecated
    public Builder endpoint(final String name,
                            final String protocol,
                            final int port) {
      endpoints.add(new Endpoint(name, protocol, port, "", ""));
      return this;
    }

    public Builder endpoint(final String name,
                            final String protocol,
                            final int port,
                            final String domain,
                            final String host) {
      endpoints.add(new Endpoint(name, protocol, port, domain, host));
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
    /** The hostname on which we will advertise this service in service discovery */
    private final String host;

    public Endpoint(final String name, final String protocol, final int port,
                    final String domain, final String host) {
      this.name = name;
      this.protocol = protocol;
      this.port = port;
      this.domain = domain;
      this.host = host;
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
  }
}
