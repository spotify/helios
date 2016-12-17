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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import javax.annotation.Nullable;

/**
 * An {@link Iterator} of {@link Endpoint} that restarts when it reaches the end.
 *
 * <p>The iterator will loop continuously around the provided elements, unless there are no elements
 * in the collection to begin with.
 */
class EndpointIterator implements Iterator<Endpoint> {

  private final List<Endpoint> endpoints;
  private final int size;
  private int cursor;

  private EndpointIterator(final List<Endpoint> endpoints) {
    this.endpoints = ImmutableList.copyOf(checkNotNull(endpoints));
    size = this.endpoints.size();
    // Set the cursor to a random location within the backing list.
    // TODO (dxia) It'd be nice to enforce the backing list to not be empty.
    // But this breaks an existing test.
    cursor = size == 0 ? 0 : new Random().nextInt(size);
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
    if (size == 0) {
      throw new NoSuchElementException();
    }

    return cursor < size ? endpoints.get(cursor++) : endpoints.get(cursor = 0);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return true if any endpoints' scheme is HTTPS.
   */
  boolean hasHttps() {
    return Iterables.any(endpoints, new Predicate<Endpoint>() {
      @Override
      public boolean apply(@Nullable Endpoint endpoint) {
        return endpoint != null && endpoint.getUri() != null
               && endpoint.getUri().getScheme() != null
               && endpoint.getUri().getScheme().equalsIgnoreCase("https");
      }
    });
  }
}
