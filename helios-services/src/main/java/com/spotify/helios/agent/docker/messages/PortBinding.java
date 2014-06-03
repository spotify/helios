package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class PortBinding {

  @JsonProperty("HostIp") private String hostIp;
  @JsonProperty("HostPort") private String hostPort;

  public String hostIp() {
    return hostIp;
  }

  public void hostIp(final String hostIp) {
    this.hostIp = hostIp;
  }

  public String hostPort() {
    return hostPort;
  }

  public void hostPort(final String hostPort) {
    this.hostPort = hostPort;
  }

  public static PortBinding of(final String ip, final String port) {
    final PortBinding binding = new PortBinding();
    binding.hostIp(ip);
    binding.hostPort(port);
    return binding;
  }

  public static PortBinding of(final String ip, final int port) {
    return of(ip, String.valueOf(port));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final PortBinding that = (PortBinding) o;

    if (hostIp != null ? !hostIp.equals(that.hostIp) : that.hostIp != null) {
      return false;
    }
    if (hostPort != null ? !hostPort.equals(that.hostPort) : that.hostPort != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostIp != null ? hostIp.hashCode() : 0;
    result = 31 * result + (hostPort != null ? hostPort.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("hostIp", hostIp)
        .add("hostPort", hostPort)
        .toString();
  }
}
