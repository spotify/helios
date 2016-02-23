/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.spotify.helios.common.descriptors.PortMapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Set;

/**
 * A simple port allocator. Given a port range and a set of used ports it will linearly search
 * through the port range until it finds an available port and claim it.
 *
 * The index into the port range is kept between calls to {@link #allocate(Map, Set)}. Successive
 * allocations will not reuse an available port until the port range has been exhausted and the
 * index wraps around from the start of the port range.
 */
public class PortAllocator {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  /**
   * Index for port allocation. Reused between allocations so we do not immediately reuse ports.
   */
  private int i;

  private final int start;
  private final int end;

  public PortAllocator(final int start, final int end) {
    this.start = start;
    this.end = end;
    this.i = start;
  }

  /**
   * Allocate ports for port mappings with no external ports configured.
   *
   * @param ports A map of port mappings for a container, both with statically configured
   *              external ports and dynamic unconfigured external ports.
   * @param used  A set of used ports. The ports allocated will not clash with these ports.
   * @return The allocated ports.
   */
  public Map<String, Integer> allocate(final Map<String, PortMapping> ports,
                                       final Set<Integer> used) {
    return allocate0(ports, Sets.newHashSet(used));
  }

  private Map<String, Integer> allocate0(final Map<String, PortMapping> mappings,
                                         final Set<Integer> used) {

    final ImmutableMap.Builder<String, Integer> allocation = ImmutableMap.builder();

    for (Map.Entry<String, PortMapping> entry : mappings.entrySet()) {
      final String name = entry.getKey();
      final PortMapping portMapping = entry.getValue();
      final Integer externalPort = portMapping.getExternalPort();

      if (externalPort == null) {
        if (!allocateDynamic(allocation, used, name)) {
          return null;
        }
      } else {
        if (!allocateStatic(allocation, used, name, externalPort)) {
          return null;
        }
      }
    }

    return allocation.build();
  }

  private boolean allocateStatic(final ImmutableMap.Builder<String, Integer> allocation,
                              final Set<Integer> used,
                              final String name,
                              final Integer port) {
    // Verify that this port is not in use
    if (used.contains(port)) {
      return false;
    }

    used.add(port);
    allocation.put(name, port);
    return true;
  }

  private boolean allocateDynamic(final ImmutableMap.Builder<String, Integer> allocation,
                               final Set<Integer> used,
                               final String name) {
    // Look for an available port, checking at most (end - start) ports.
    for (int i = this.start; i < this.end; i++) {
      final int port = next();
      if (!used.contains(port) && portAvailable(port)) {
        used.add(port);
        allocation.put(name, port);
        return true;
      }
    }
    return false;
  }

  /**
   * Get the next port number to try, continuing from the previous port allocation to avoid eagerly
   * reusing ports. Wraps around when the end of the port range has been reached.
   *
   * @return The next port.
   */
  private int next() {
    if (i == end) {
      i = start;
    }
    return i++;
  }

  /**
   * Check if the port is available on the host. This is racy but it's better than nothing.
   * @param port Port number to check.
   * @return True if port is available. False otherwise.
   */
  private boolean portAvailable(final int port) {
    ServerSocket s = null;
    try {
      s = new ServerSocket(port);
      return true;
    } catch (IOException ignored) {
      return false;
    } finally {
      if (s != null) {
        try {
          s.close();
        } catch (IOException e) {
          log.error("Couldn't close socket on port {} when checking availability: {}", port, e);
        }
      }
    }
  }
}
