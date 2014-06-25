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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static java.lang.String.format;
import static java.lang.System.getenv;

public abstract class Resolver {

  private static final String SRV_FORMAT = env("HELIOS_SRV_FORMAT", "_%s._http.%s");

  private static String env(final String name, final String defaultValue) {
    return Optional.fromNullable(getenv(name)).or(defaultValue);
  }

  private static final Logger log = LoggerFactory.getLogger(Resolver.class);

  public static Supplier<List<URI>> supplier(final String srvName, final String domain) {
    return new Supplier<List<URI>>() {
      @Override
      public List<URI> get() {
        return resolve(srvName, domain);
      }
    };
  }

  public static List<URI> resolve(final String srvName, final String domain) {
    final String name = srv(srvName, domain);
    final Lookup lookup;
    try {
      lookup = new Lookup(name, Type.SRV, DClass.IN);
    } catch (TextParseException e) {
      throw new IllegalArgumentException("unable to create lookup for name: " + name, e);
    }

    Record[] queryResult = lookup.run();

    switch (lookup.getResult()) {
      case Lookup.SUCCESSFUL:
        final ImmutableList.Builder<URI> endpoints = ImmutableList.builder();
        for (Record record : queryResult) {
          if (record instanceof SRVRecord) {
            SRVRecord srv = (SRVRecord) record;
            endpoints.add(http(srv.getTarget().toString(), srv.getPort()));
          }
        }
        return endpoints.build();
      case Lookup.HOST_NOT_FOUND:
        // fallthrough
      case Lookup.TYPE_NOT_FOUND:
        log.warn("No results returned for query '{}'", name);
        return ImmutableList.of();
      default:
        throw new HeliosRuntimeException(String.format("Lookup of '%s' failed with code: %d - %s ",
                                                       name, lookup.getResult(),
                                                       lookup.getErrorString()));
    }
  }

  private static URI http(final String host, final int port) {
    final URI endpoint;
    try {
      endpoint = new URI("http", null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
    return endpoint;
  }

  private static String srv(final String name, final String domain) {
    return format(SRV_FORMAT, name, domain);
  }
}
