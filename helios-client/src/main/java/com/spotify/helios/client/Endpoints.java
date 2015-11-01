/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.client;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * A class that provides static factory methods for {@link Endpoint}.
 */
public class Endpoints {

  private static final Logger log = LoggerFactory.getLogger(Endpoints.class);

  private static final DnsResolver DEFAULT_DNS_RESOLVER = SystemDefaultDnsResolver.INSTANCE;

  private Endpoints() {
  }

  /**
   * Returns a {@link Supplier} of a list of {@link Endpoint}.
   * @param uriSupplier A Supplier of a list of URIs.
   * @return A Supplier of a list of Endpoints.
   */
  public static Supplier<List<Endpoint>> of(final Supplier<List<URI>> uriSupplier) {
    return of(uriSupplier, DEFAULT_DNS_RESOLVER);
  }

  /**
   * Returns a {@link Supplier} of a list of {@link Endpoint}.
   * @param uriSupplier A Supplier of a list of URIs.
   * @param dnsResolver An instance of {@link DnsResolver}
   * @return A Supplier of a list of Endpoints.
   */
  static Supplier<List<Endpoint>> of(final Supplier<List<URI>> uriSupplier,
                                     final DnsResolver dnsResolver) {
    return new Supplier<List<Endpoint>>() {
      @Override
      public List<Endpoint> get() {
        return of(uriSupplier.get(), dnsResolver);
      }
    };
  }

  /**
   * Returns a list of {@link Endpoint}.
   * @param uris A list of URIs.
   * @return A list of Endpoints.
   */
  public static List<Endpoint> of(final List<URI> uris) {
    return of(uris, DEFAULT_DNS_RESOLVER);
  }

  /**
   * Returns a list of {@link Endpoint}.
   * @param uris A list of URIs.
   * @param dnsResolver An instance of {@link DnsResolver}
   * @return A list of Endpoints.
   */
  static List<Endpoint> of(final List<URI> uris, final DnsResolver dnsResolver) {
    final ImmutableList.Builder<Endpoint> endpoints = ImmutableList.builder();
    for (final URI uri : uris) {
      try {
        endpoints.add(new DefaultEndpoint(uri, Arrays.asList(dnsResolver.resolve(uri.getHost()))));
      } catch (UnknownHostException e) {
        log.warn("Unable to resolve hostname {} into IP address: {}", uri.getHost(), e);
      }
    }

    return endpoints.build();
  }

  private static class DefaultEndpoint implements Endpoint {

    private final List<InetAddress> ips;
    private final URI uri;

    DefaultEndpoint(final URI uri, final List<InetAddress> ips) {
      this.uri = uri;
      this.ips = ips;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public List<InetAddress> getIps() {
      return ips;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DefaultEndpoint that = (DefaultEndpoint) o;

      if (ips != null ? !ips.equals(that.ips) : that.ips != null) {
        return false;
      }
      return !(uri != null ? !uri.equals(that.uri) : that.uri != null);

    }

    @Override
    public int hashCode() {
      int result = ips != null ? ips.hashCode() : 0;
      result = 31 * result + (uri != null ? uri.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("uri", uri)
          .add("ips", ips)
          .toString();
    }
  }
}
