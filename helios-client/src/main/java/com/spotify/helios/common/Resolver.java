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

package com.spotify.helios.common;

import static java.lang.String.format;
import static java.lang.System.getenv;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that resolves DNS SRV records. Used by {@link PeriodicResolver} to discover
 * helios masters.
 */
class Resolver {

  private static final Logger log = LoggerFactory.getLogger(Resolver.class);

  private static final String HTTPS_SRV_FORMAT = env("HELIOS_HTTPS_SRV_FORMAT", "_%s._https.%s");
  private static final String HTTP_SRV_FORMAT = env("HELIOS_HTTP_SRV_FORMAT", "_%s._http.%s");

  private static final DnsSrvResolver DEFAULT_RESOLVER = DnsSrvResolvers.newBuilder().build();

  private static String env(final String name, final String defaultValue) {
    return Optional.fromNullable(getenv(name)).or(defaultValue);
  }

  List<URI> resolve(final String srvName, final String domain) {
    return resolve(srvName, domain, DEFAULT_RESOLVER);
  }

  List<URI> resolve(final String srvName, final String domain, final DnsSrvResolver resolver) {
    List<URI> uris = resolve(srvName, "https", domain, resolver);
    if (uris.isEmpty()) {
      return resolve(srvName, "http", domain, resolver);
    }
    return uris;
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

    final ImmutableList<URI> uris = endpoints.build();
    log.info("Resolved {} to {}", name, uris);
    return uris;
  }

  private static URI protocol(final String protocol, final String host, final int port) {
    final URI endpoint;
    try {
      endpoint = new URI(protocol, null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
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
