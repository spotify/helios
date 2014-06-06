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

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.spotify.helios.common.descriptors.PortMapping;

import java.util.Map;
import java.util.Set;

public class PortAllocator {

  private int i;

  private final int start;
  private final int end;

  public PortAllocator(final int start, final int end) {
    this.start = start;
    this.end = end;
    this.i = start;
  }

  public Map<String, Integer> allocate(final Map<String, PortMapping> ports,
                                       final Set<Integer> used) {
    return allocate0(ports, Sets.newHashSet(used));
  }

  private Map<String, Integer> allocate0(final Map<String, PortMapping> mappings,
                                         final Set<Integer> used) {

    final ImmutableMap.Builder<String, Integer> allocation = ImmutableMap.builder();

    // Allocate static ports
    for (Map.Entry<String, PortMapping> entry : mappings.entrySet()) {
      final String name = entry.getKey();
      final PortMapping portMapping = entry.getValue();
      final Integer externalPort = portMapping.getExternalPort();

      // Skip dynamic ports
      if (externalPort == null) {
        continue;
      }

      // Verify that this port is not in use
      if (used.contains(externalPort)) {
        return null;
      }
      used.add(externalPort);
      allocation.put(name, externalPort);
    }

    // Allocate dynamic ports
    for (Map.Entry<String, PortMapping> entry : mappings.entrySet()) {
      final String name = entry.getKey();
      final PortMapping portMapping = entry.getValue();
      final Integer externalPort = portMapping.getExternalPort();

      // Skip static ports
      if (externalPort != null) {
        continue;
      }

      // Look for an available port
      Integer port = null;
      for (int i = start; i < end; i++) {
        final int candidate = next();
        if (!used.contains(candidate)) {
          port = candidate;
          break;
        }
      }
      if (port == null) {
        return null;
      }
      used.add(port);
      allocation.put(name, port);
    }

    return allocation.build();
  }

  private int next() {
    if (i == end) {
      i = start;
    }
    return i++;
  }
}
