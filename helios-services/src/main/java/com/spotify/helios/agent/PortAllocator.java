/**
 * Copyright (C) 2014 Spotify AB
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
