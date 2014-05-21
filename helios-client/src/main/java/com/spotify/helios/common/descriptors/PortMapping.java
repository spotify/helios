/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

public class PortMapping extends Descriptor {

  public static final String TCP = "tcp";
  public static final String UDP = "udp";

  private final int internalPort;
  private final Integer externalPort;
  private final String protocol;

  public PortMapping(@JsonProperty("internalPort") final int internalPort,
                     @JsonProperty("externalPort") final Integer externalPort,
                     @JsonProperty("protocol") final String protocol) {
    this.internalPort = internalPort;
    this.externalPort = externalPort;
    this.protocol = protocol;
  }

  public PortMapping(final int internalPort, final Integer externalPort) {
    this.internalPort = internalPort;
    this.externalPort = externalPort;
    this.protocol = TCP;
  }

  public PortMapping(final int internalPort) {
    this.internalPort = internalPort;
    this.externalPort = null;
    this.protocol = TCP;
  }

  public int getInternalPort() {
    return internalPort;
  }

  @JsonInclude(ALWAYS)
  @Nullable
  public Integer getExternalPort() {
    return externalPort;
  }

  @JsonInclude(ALWAYS)
  public String getProtocol() {
    return protocol;
  }

  public static PortMapping of(final int internalPort) {
    return new PortMapping(internalPort);
  }

  public static PortMapping of(final int internalPort, final Integer externalPort) {
    return new PortMapping(internalPort, externalPort);
  }

  public static PortMapping of(final int internalPort, final Integer externalPort,
                               final String protocol) {
    return new PortMapping(internalPort, externalPort, protocol);
  }

  public static PortMapping of(final int internalPort, final String protocol) {
    return new PortMapping(internalPort, null, protocol);
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
    if (protocol != null ? !protocol.equals(that.protocol) : that.protocol != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = internalPort;
    result = 31 * result + (externalPort != null ? externalPort.hashCode() : 0);
    result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("internalPort", internalPort)
        .add("externalPort", externalPort)
        .add("protocol", protocol)
        .toString();
  }
}
