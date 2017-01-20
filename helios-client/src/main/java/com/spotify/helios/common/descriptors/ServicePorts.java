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

package com.spotify.helios.common.descriptors;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static java.util.Arrays.asList;

/**
 * A set of port names, that is map keys, together with ServicePortParameters containing optional
 * metadata for a port.
 *
 * A typical JSON representation might be:
 * <pre>
 * {
 *   "http" : { }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServicePorts extends Descriptor {

  private final Map<String, ServicePortParameters> ports;

  public ServicePorts(@JsonProperty("ports") final Map<String, ServicePortParameters> ports) {
    this.ports = ports;
  }

  public Map<String, ServicePortParameters> getPorts() {
    return ports;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServicePorts that = (ServicePorts) o;

    if (ports != null ? !ports.equals(that.ports) : that.ports != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return ports != null ? ports.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "ServicePorts{" +
           "ports=" + ports +
           '}';
  }

  public static ServicePorts of(final String... ports) {
    return of(asList(ports));
  }

  private static ServicePorts of(final Iterable<String> ports) {
    final ImmutableMap.Builder<String, ServicePortParameters> builder = ImmutableMap.builder();
    for (final String port : ports) {
      builder.put(port, new ServicePortParameters(null));
    }
    return new ServicePorts(builder.build());
  }
}
