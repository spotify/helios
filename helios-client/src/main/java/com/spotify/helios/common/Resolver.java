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

package com.spotify.helios.common;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static java.lang.String.format;
import static java.lang.System.getenv;

public abstract class Resolver {

  private static final String HTTPS_SRV_FORMAT = env("HELIOS_HTTPS_SRV_FORMAT", "_%s._https.%s");
  private static final String HTTP_SRV_FORMAT = env("HELIOS_HTTP_SRV_FORMAT", "_%s._http.%s");

  private static final DnsSrvResolver DEFAULT_RESOLVER = DnsSrvResolvers.newBuilder().build();

  private static String env(final String name, final String defaultValue) {
    return Optional.fromNullable(getenv(name)).or(defaultValue);
  }

  public static Supplier<List<URI>> supplier(final String srvName, final String domain) {
    return supplier(srvName, domain, DEFAULT_RESOLVER);
  }

  static Supplier<List<URI>> supplier(final String srvName, final String domain,
                                      final DnsSrvResolver resolver) {
    return new Supplier<List<URI>>() {
      @Override
      public List<URI> get() {
        // Try to get HTTPS SRV records first and fallback to HTTP
        List<URI> uris = resolve(srvName, "https", domain, resolver);
        if (uris.isEmpty()) {
          uris = resolve(srvName, "http", domain, resolver);
        }
        return uris;
      }
    };
  }

  private static List<URI> resolve(final String srvName,
                                   final String protocol,
                                   final String domain,
                                   final DnsSrvResolver resolver) {
    final String name;
    switch (protocol) {
      case "https":
        name = httpsSrv(srvName, domain);
        break;
      case "http":
        name = httpSrv(srvName, domain);
        break;
      default:
        throw new IllegalArgumentException(String.format(
            "Invalid protocol: %s. Helios SRV record can only be https or http.", protocol));
    }

    final List<LookupResult> lookupResults = resolver.resolve(name);

    final ImmutableList.Builder<URI> endpoints = ImmutableList.builder();
    for (final LookupResult result : lookupResults) {
      endpoints.add(protocol(protocol, result.host(), result.port()));
    }

    return endpoints.build();
  }

  private static URI protocol(final String protocol, final String host, final int port) {
    final URI endpoint;
    try {
      endpoint = new URI(protocol, null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
    return endpoint;
  }

  private static String httpsSrv(final String name, final String domain) {
    return format(HTTPS_SRV_FORMAT, name, domain);
  }

  private static String httpSrv(final String name, final String domain) {
    return format(HTTP_SRV_FORMAT, name, domain);
  }
}
