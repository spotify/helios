/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.common.Resolver;
import java.net.URI;
import java.util.List;

/**
 * A target cluster identified by an endpoint string that can be used with a {@link
 * com.spotify.helios.client.HeliosClient}.
 */
public abstract class Target {
  private final String name;

  Target(final String name) {
    this.name = name;
  }

  public abstract Supplier<List<URI>> getEndpointSupplier();

  public String getName() {
    return name;
  }

  private static class SrvTarget extends Target {
    private final String srv;
    private final String domain;

    private SrvTarget(final String srv, final String domain) {
      super(domain);
      this.srv = srv;
      this.domain = domain;
    }

    public String getSrv() {
      return srv;
    }

    public String getDomain() {
      return domain;
    }

    @Override
    public Supplier<List<URI>> getEndpointSupplier() {
      return Resolver.supplier(srv, domain);
    }

    @Override
    public String toString() {
      return domain + " (srv: " + srv + ")";
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      final SrvTarget srvTarget = (SrvTarget) obj;

      if (!srv.equals(srvTarget.getSrv())) {
        return false;
      }
      if (!domain.equals(srvTarget.getDomain())) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = srv != null ? srv.hashCode() : 0;
      result = 31 * result + (domain != null ? domain.hashCode() : 0);
      return result;
    }
  }

  private static class ExplicitTarget extends Target {
    private final URI endpoint;

    private ExplicitTarget(final URI endpoint) {
      super(endpoint.toString());
      this.endpoint = endpoint;
    }

    public URI getEndpoint() {
      return endpoint;
    }

    @Override
    public Supplier<List<URI>> getEndpointSupplier() {
      final List<URI> endpoints = ImmutableList.of(endpoint);
      return Suppliers.ofInstance(endpoints);
    }

    @Override
    public String toString() {
      return endpoint.toString();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      final ExplicitTarget explicitTarget = (ExplicitTarget) obj;

      return endpoint.equals(explicitTarget.getEndpoint());

    }

    @Override
    public int hashCode() {
      return endpoint != null ? endpoint.hashCode() : 0;
    }
  }

  /**
   * Create a target from an explicit endpoint
   *
   * @param endpoint The endpoint.
   *
   * @return The target.
   */
  public static Target from(final URI endpoint) {
    return new ExplicitTarget(endpoint);
  }

  /**
   * Create targets for a list of domains
   *
   * @param srvName The SRV name.
   * @param domains A list of domains.
   *
   * @return A list of targets.
   */
  public static List<Target> from(final String srvName, final Iterable<String> domains) {
    final ImmutableList.Builder<Target> builder = ImmutableList.builder();
    for (final String domain : domains) {
      builder.add(new SrvTarget(srvName, domain));
    }
    return builder.build();
  }
}
