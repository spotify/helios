/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;
import static java.util.Arrays.asList;

public class ServicePorts extends Descriptor {

  private final Map<String, ServicePortParameters> ports;

  public ServicePorts(@JsonProperty("ports") final Map<String, ServicePortParameters> ports) {
    this.ports = ports;
  }

  @JsonInclude(ALWAYS)
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
    return Objects.toStringHelper(this)
        .add("ports", ports)
        .toString();
  }

  public static ServicePorts of(final String... ports) {
    return of(asList(ports));
  }

  private static ServicePorts of(final Iterable<String> ports) {
    final ImmutableMap.Builder<String, ServicePortParameters> builder = ImmutableMap.builder();
    for (final String port : ports) {
      builder.put(port, new ServicePortParameters());
    }
    return new ServicePorts(builder.build());
  }
}
