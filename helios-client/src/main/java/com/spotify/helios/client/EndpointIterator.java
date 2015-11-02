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

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An {@link Iterator} of {@link Endpoint} that restarts when it reaches the end.
 *
 * The iterator will loop continuously around the provided elements, unless there are no elements
 * in the collection to begin with.
 */
class EndpointIterator implements Iterator<Endpoint> {

  private final List<Endpoint> endpoints;
  private final int size;
  private int cursor;

  private EndpointIterator(final List<Endpoint> endpoints) {
    checkNotNull(endpoints);
    if (endpoints.size() == 0) {
      throw new IllegalArgumentException("List of Endpoints cannot be empty.");
    }

    this.endpoints = endpoints;
    this.size = endpoints.size();
    this.cursor = 0;
  }

  static EndpointIterator of(final List<Endpoint> endpoints) {
    return new EndpointIterator(endpoints);
  }

  @Override
  public boolean hasNext() {
    return size > 0;
  }

  @Override
  public Endpoint next() {
    return cursor < size ?
           endpoints.get(cursor++) :
           endpoints.get(cursor = 0);
  }

  public int size() {
    return size;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
