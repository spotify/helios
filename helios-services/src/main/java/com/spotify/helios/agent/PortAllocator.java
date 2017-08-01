/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.spotify.helios.common.descriptors.PortMapping;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple port allocator.
 *
 * <p>Given a port range and a set of used ports it will randomly search through the port range
 * until it finds an available port and claim it. Static ports are simply checked against the used
 * ports.
 */
public class PortAllocator {

  private static final Logger log = LoggerFactory.getLogger(Agent.class);

  private int idx = 0;
  private final List<Integer> potentialPorts;

  public PortAllocator(final int start, final int end) {
    this.potentialPorts = IntStream.range(start, end)
        .boxed()
        .collect(Collectors.toList());
    Collections.shuffle(this.potentialPorts);
  }

  /**
   * Allocate ports for port mappings with no external ports configured.
   *
   * @param ports A map of port mappings for a container, both with statically configured
   *              external ports and dynamic unconfigured external ports.
   * @param used  A set of used ports. The ports allocated will not clash with these ports.
   *
   * @return The allocated ports.
   */
  public Map<String, Integer> allocate(final Map<String, PortMapping> ports,
                                       final Set<Integer> used) {
    return allocate0(ports, Sets.newHashSet(used));
  }

  private Map<String, Integer> allocate0(final Map<String, PortMapping> mappings,
                                         final Set<Integer> used) {

    final ImmutableMap.Builder<String, Integer> allocation = ImmutableMap.builder();

    for (final Map.Entry<String, PortMapping> entry : mappings.entrySet()) {
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
    for (int i = 0; i < this.potentialPorts.size(); i++) {
      final Integer port = nextPotentialPort();
      if (!used.contains(port) && portAvailable(port)) {
        used.add(port);
        allocation.put(name, port);
        return true;
      }
    }
    return false;
  }

  private Integer nextPotentialPort() {
    if (this.idx >= this.potentialPorts.size()) {
      this.idx = 0;
    }

    final Integer nextPort = this.potentialPorts.get(this.idx);
    this.idx++;

    return nextPort;
  }

  /**
   * Check if the port is available on the host. This is racy but it's better than nothing.
   *
   * @param port Port number to check.
   *
   * @return True if port is available. False otherwise.
   */
  private boolean portAvailable(final int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(port);
      return true;
    } catch (IOException ignored) {
      return false;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          log.error("Couldn't close socket on port {} when checking availability: {}", port, e);
        }
      }
    }
  }
}
