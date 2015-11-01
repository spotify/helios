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

import com.google.common.collect.ImmutableMap;

import java.net.InetAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} of {@link Endpoint}.
 * The hasNext() and next() methods will return whether the iterator has a next Endpoint and the
 * next endpoint, respectively. In addition, the hasNextIp() and nextIp() methods iterate over
 * the current Endpoint's {@link InetAddress}s. I.e. they return whether the current endpoint
 * has more IP addresses and the next IP address for that endpoint, respectively.
 */
class EndpointIterator implements Iterator<Endpoint> {

  private final List<Endpoint> endpoints;
  private final int numEndpoints;
  private final Map<URI, Integer> numIps;
  private int endpointCursor;
  private int ipCursor;

  private EndpointIterator(final List<Endpoint> endpoints) {
    this.endpoints = endpoints;
    this.numEndpoints = endpoints.size();

    // Calculate the number of IPs for each endpoint once here so we don't have to do it in every
    // call to hasNextIp().
    final ImmutableMap.Builder<URI, Integer> builder = ImmutableMap.builder();
    for (final Endpoint e : endpoints) {
      builder.put(e.getUri(), e.getIps().size());
    }
    numIps = builder.build();

    this.endpointCursor = 0;
    this.ipCursor = 0;
  }

  static EndpointIterator of(final List<Endpoint> endpoints) {
    return new EndpointIterator(endpoints);
  }

  @Override
  public boolean hasNext() {
    return endpointCursor < numEndpoints;
  }

  public boolean hasNextIp() {
    return numEndpoints > 0 && ipCursor < numIps.get(endpoints.get(endpointCursor).getUri());
  }

  @Override
  public Endpoint next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    } else {
      return endpoints.get(ipCursor++);
    }
  }

  public InetAddress nextIp() {
    if (!this.hasNextIp()) {
      throw new NoSuchElementException();
    } else {
      return endpoints.get(endpointCursor).getIps().get(ipCursor++);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
