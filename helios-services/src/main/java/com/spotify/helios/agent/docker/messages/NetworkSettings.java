package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class NetworkSettings {

  @JsonProperty("IPAddress") private String ipAddress;
  @JsonProperty("IPPrefixLen") private int ipPrefixLen;
  @JsonProperty("Gateway") private String gateway;
  @JsonProperty("Bridge") private String bridge;
  @JsonProperty("PortMapping") private Map<String, Map<String, String>> portMapping;
  @JsonProperty("Ports") private Map<String, List<PortBinding>> ports;

  public String ipAddress() {
    return ipAddress;
  }

  public void ipAddress(final String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public int ipPrefixLen() {
    return ipPrefixLen;
  }

  public void ipPrefixLen(final int ipPrefixLen) {
    this.ipPrefixLen = ipPrefixLen;
  }

  public String gateway() {
    return gateway;
  }

  public void gateway(final String gateway) {
    this.gateway = gateway;
  }

  public String bridge() {
    return bridge;
  }

  public void bridge(final String bridge) {
    this.bridge = bridge;
  }

  public Map<String, Map<String, String>> portMapping() {
    return portMapping;
  }

  public void portMapping(final Map<String, Map<String, String>> portMapping) {
    this.portMapping = portMapping;
  }

  public Map<String, List<PortBinding>> ports() {
    return ports;
  }

  public void ports(final Map<String, List<PortBinding>> ports) {
    this.ports = ports;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final NetworkSettings that = (NetworkSettings) o;

    if (ipPrefixLen != that.ipPrefixLen) {
      return false;
    }
    if (bridge != null ? !bridge.equals(that.bridge) : that.bridge != null) {
      return false;
    }
    if (gateway != null ? !gateway.equals(that.gateway) : that.gateway != null) {
      return false;
    }
    if (ipAddress != null ? !ipAddress.equals(that.ipAddress) : that.ipAddress != null) {
      return false;
    }
    if (portMapping != null ? !portMapping.equals(that.portMapping) : that.portMapping != null) {
      return false;
    }
    if (ports != null ? !ports.equals(that.ports) : that.ports != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = ipAddress != null ? ipAddress.hashCode() : 0;
    result = 31 * result + ipPrefixLen;
    result = 31 * result + (gateway != null ? gateway.hashCode() : 0);
    result = 31 * result + (bridge != null ? bridge.hashCode() : 0);
    result = 31 * result + (portMapping != null ? portMapping.hashCode() : 0);
    result = 31 * result + (ports != null ? ports.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("ipAddress", ipAddress)
        .add("ipPrefixLen", ipPrefixLen)
        .add("gateway", gateway)
        .add("bridge", bridge)
        .add("portMapping", portMapping)
        .add("ports", ports)
        .toString();
  }

}