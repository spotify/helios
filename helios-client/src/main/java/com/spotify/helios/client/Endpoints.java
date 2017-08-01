/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.client;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   *
   * @param uriSupplier A Supplier of a list of URIs.
   *
   * @return A Supplier of a list of Endpoints.
   */
  public static Supplier<List<Endpoint>> of(final Supplier<List<URI>> uriSupplier) {
    return of(uriSupplier, DEFAULT_DNS_RESOLVER);
  }

  /**
   * Returns a {@link Supplier} of a list of {@link Endpoint}.
   *
   * @param uriSupplier A Supplier of a list of URIs.
   * @param dnsResolver An instance of {@link DnsResolver}
   *
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
   *
   * @param uris A list of URIs.
   *
   * @return A list of Endpoints.
   */
  public static List<Endpoint> of(final List<URI> uris) {
    return of(uris, DEFAULT_DNS_RESOLVER);
  }

  /**
   * Returns a list of {@link Endpoint}.
   *
   * @param uris        A list of URIs.
   * @param dnsResolver An instance of {@link DnsResolver}
   *
   * @return A list of Endpoints.
   */
  static List<Endpoint> of(final List<URI> uris, final DnsResolver dnsResolver) {
    final ImmutableList.Builder<Endpoint> endpoints = ImmutableList.builder();
    for (final URI uri : uris) {
      try {
        for (final InetAddress ip : dnsResolver.resolve(uri.getHost())) {
          endpoints.add(new DefaultEndpoint(uri, ip));
        }
      } catch (UnknownHostException e) {
        log.warn("Unable to resolve hostname {} into IP address", uri.getHost(), e);
      }
    }

    return endpoints.build();
  }

  private static class DefaultEndpoint implements Endpoint {

    private static final List<String> VALID_PROTOCOLS = ImmutableList.of("http", "https");
    private static final String VALID_PROTOCOLS_STR =
        String.format("[%s]", Joiner.on("|").join(VALID_PROTOCOLS));

    private final InetAddress ip;
    private final URI uri;

    DefaultEndpoint(final URI uri, final InetAddress ip) {
      this.uri = checkNotNull(uri);
      this.ip = checkNotNull(ip);

      final String scheme = this.uri.getScheme();
      final String host = this.uri.getHost();
      final int port = this.uri.getPort();
      if (!VALID_PROTOCOLS.contains(scheme) || host == null || port == -1) {
        throw new IllegalArgumentException(String.format(
            "Master endpoints must be of the form \"%s://<hostname>:<port>\"",
            VALID_PROTOCOLS_STR));
      }
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public InetAddress getIp() {
      return ip;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      final DefaultEndpoint that = (DefaultEndpoint) obj;

      //noinspection SimplifiableIfStatement
      if (ip != null ? !ip.equals(that.ip) : that.ip != null) {
        return false;
      }
      return !(uri != null ? !uri.equals(that.uri) : that.uri != null);

    }

    @Override
    public int hashCode() {
      int result = ip != null ? ip.hashCode() : 0;
      result = 31 * result + (uri != null ? uri.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "DefaultEndpoint{"
             + "ip=" + ip
             + ", uri=" + uri
             + '}';
    }
  }
}
