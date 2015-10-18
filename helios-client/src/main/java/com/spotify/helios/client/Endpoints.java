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

package com.spotify.helios.client;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

public class Endpoints {

  private static final Logger log = LoggerFactory.getLogger(HeliosClient.class);

  private Endpoints() {

  }

  public static Supplier<List<Endpoint>> of(final Supplier<List<URI>> uriSupplier) {
    return new Supplier<List<Endpoint>>() {
      @Override
      public List<Endpoint> get() {
        return of(uriSupplier.get());
      }
    };
  }

  public static List<Endpoint> of(final List<URI> uris) {
    final ImmutableList.Builder<Endpoint> endpoints = ImmutableList.builder();
    for (final URI uri : uris) {
      try {
        endpoints.add(new EndpointImpl(uri, InetAddress.getAllByName(uri.getHost())));
      } catch (UnknownHostException e) {
        log.warn("Unable to resolve hostname {} into IP address: {}", uri.getHost(), e);
      }
    }

    return endpoints.build();
  }

  private static class EndpointImpl implements Endpoint {

    private final InetAddress[] ips;
    private final URI uri;

    EndpointImpl(final URI uri, final InetAddress[] ips) {
      this.uri = uri;
      this.ips = ips;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public InetAddress[] getIps() {
      return ips;
    }
  }
}
