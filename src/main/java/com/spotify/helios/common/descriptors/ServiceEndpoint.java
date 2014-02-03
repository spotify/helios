package com.spotify.helios.common.descriptors;

import com.google.common.base.Splitter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceEndpoint extends Descriptor implements Comparable<ServiceEndpoint> {

  public static final String TCP = "tcp";
  public static final String HERMES = "hm";

  private final String name;
  private final String protocol;

  public ServiceEndpoint(final String s) {
    final List<String> parts = Splitter.on('/').splitToList(s);
    if (parts.size() < 1 || parts.size() > 2) {
      throw new IllegalArgumentException();
    }
    name = parts.get(0);
    protocol = parts.size() > 1 ? parts.get(1) : HERMES;
  }

  public ServiceEndpoint(@JsonProperty("name") final String name,
                         @JsonProperty("protocol") final String protocol) {
    this.name = name;
    this.protocol = protocol;
  }

  public String getName() {
    return name;
  }

  public String getProtocol() {
    return protocol;
  }

  @Override
  public int compareTo(final ServiceEndpoint o) {
    return toString().compareTo(o.toString());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServiceEndpoint that = (ServiceEndpoint) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (protocol != null ? !protocol.equals(that.protocol) : that.protocol != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return name + "/" + protocol;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static ServiceEndpoint of(final String service, final String proto) {
    return new ServiceEndpoint(service, proto);
  }

  public static class Builder {

    private String name;
    private String protocol;

    public Builder() {
    }

    private Builder(final ServiceEndpoint serviceEndpoint) {
      this.name = serviceEndpoint.name;
      this.protocol = serviceEndpoint.protocol;
    }

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setProtocol(final String protocol) {
      this.protocol = protocol;
      return this;
    }

    public ServiceEndpoint build() {
      return new ServiceEndpoint(name, protocol);
    }
  }
}
