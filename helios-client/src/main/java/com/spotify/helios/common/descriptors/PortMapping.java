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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.net.InetAddresses;

import org.jetbrains.annotations.Nullable;


/**
 * Represents a mapping from a port inside the container to be exposed on the agent.
 *
 * <p>A typical JSON representation might be:
 * <pre>
 * {
 *   "ip": "0.0.0.0",
 *   "externalPort" : 8061,
 *   "internalPort" : 8081,
 *   "protocol" : "tcp"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PortMapping extends Descriptor {

  public static final String WILDCARD_ADDRESS = "0.0.0.0";
  public static final String TCP = "tcp";
  public static final String UDP = "udp";

  private final String ip;
  private final int internalPort;
  private final Integer externalPort;
  private final String protocol;

  public PortMapping(@JsonProperty("ip") @Nullable final String ip,
                     @JsonProperty("internalPort") final int internalPort,
                     @JsonProperty("externalPort") @Nullable final Integer externalPort,
                     @JsonProperty("protocol") @Nullable final String protocol) {
    this.ip = Optional.fromNullable(ip).or(WILDCARD_ADDRESS);
    // Validate IP here instead of in PortMappingParser to guaruntee every instance has a valid IP
    if (!InetAddresses.isInetAddress(this.ip)) {
      throw new IllegalArgumentException(ip + " is not a valid IP address.");
    }

    this.internalPort = internalPort;
    this.externalPort = externalPort;
    this.protocol = Optional.fromNullable(protocol).or(TCP);
  }

  public PortMapping(final int internalPort, final Integer externalPort) {
    this(WILDCARD_ADDRESS, internalPort, externalPort, TCP);
  }

  public PortMapping(final int internalPort) {
    this(WILDCARD_ADDRESS, internalPort, null, TCP);
  }

  private PortMapping(final Builder builder) {
    this(checkNotNull(builder.ip), builder.internalPort, builder.externalPort,
        checkNotNull(builder.protocol));
  }

  @Nullable
  public String getIp() {
    return ip;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public boolean hasExternalPort() {
    return externalPort != null;
  }

  @Nullable
  public Integer getExternalPort() {
    return externalPort;
  }

  public String getProtocol() {
    return protocol;
  }

  public PortMapping withExternalPort(final Integer externalPort) {
    return PortMapping.of(internalPort, externalPort, protocol);
  }

  public static PortMapping of(final int internalPort) {
    return new PortMapping(internalPort);
  }

  public static PortMapping of(final int internalPort, final Integer externalPort) {
    return new PortMapping(internalPort, externalPort);
  }

  public static PortMapping of(final int internalPort, final Integer externalPort,
                               final String protocol) {
    return new PortMapping(WILDCARD_ADDRESS, internalPort, externalPort, protocol);
  }

  public static PortMapping of(final int internalPort, final String protocol) {
    return new PortMapping(WILDCARD_ADDRESS, internalPort, null, protocol);
  }

  public static Builder builder() {
    return new Builder().ip(WILDCARD_ADDRESS).protocol(TCP);
  }

  public static class Builder {
    private String ip;
    private int internalPort;
    private Integer externalPort;
    private String protocol;

    public Builder ip(final String ip) {
      this.ip = ip;
      return this;
    }

    public Builder internalPort(final int internalPort) {
      this.internalPort = internalPort;
      return this;
    }

    public Builder externalPort(final Integer externalPort) {
      this.externalPort = externalPort;
      return this;
    }

    public Builder protocol(final String protocol) {
      this.protocol = protocol;
      return this;
    }

    public PortMapping build() {
      return new PortMapping(this);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final PortMapping that = (PortMapping) obj;

    if (ip != null ? !ip.equals(that.ip) : that.ip != null) {
      return false;
    }
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
    result = 31 * result + (ip != null ? ip.hashCode() : 0);
    result = 31 * result + (externalPort != null ? externalPort.hashCode() : 0);
    result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "PortMapping{"
           + "ip=" + ip
           + ", internalPort=" + internalPort
           + ", externalPort=" + externalPort
           + ", protocol='" + protocol + '\''
           + '}';
  }
}
