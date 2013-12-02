/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

public class PortMapping extends Descriptor {

  private final int internalPort;
  private final Integer externalPort;

  public PortMapping(@JsonProperty("internalPort") final int internalPort,
                     @JsonProperty("externalPort") final Integer externalPort) {
    this.internalPort = internalPort;
    this.externalPort = externalPort;
  }

  public PortMapping(final int internalPort) {
    this.internalPort = internalPort;
    this.externalPort = null;
  }

  public int getInternalPort() {
    return internalPort;
  }

  @Nullable
  public Integer getExternalPort() {
    return externalPort;
  }

  public static PortMapping of(final int internalPort) {
    return new PortMapping(internalPort);
  }

  public static PortMapping of(final int internalPort, final int externalPort) {
    return new PortMapping(internalPort, externalPort);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PortMapping that = (PortMapping) o;

    if (internalPort != that.internalPort) {
      return false;
    }
    if (externalPort != null ? !externalPort.equals(that.externalPort)
                             : that.externalPort != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = internalPort;
    result = 31 * result + (externalPort != null ? externalPort.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("internalPort", internalPort)
        .add("externalPort", externalPort)
        .toString();
  }
}
