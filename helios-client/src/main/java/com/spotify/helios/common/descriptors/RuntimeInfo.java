/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RuntimeInfo extends Descriptor {

  private final String name;
  private final String vmName;
  private final String vmVendor;
  private final String vmVersion;
  private final String specName;
  private final String specVendor;
  private final String specVersion;
  private final List<String> inputArguments;
  private final long uptime;
  private final long startTime;

  public RuntimeInfo(@JsonProperty("name") final String name,
                     @JsonProperty("vmName") final String vmName,
                     @JsonProperty("vmVendor") final String vmVendor,
                     @JsonProperty("vmVersion") final String vmVersion,
                     @JsonProperty("specName") final String specName,
                     @JsonProperty("specVendor") final String specVendor,
                     @JsonProperty("specVersion") final String specVersion,
                     @JsonProperty("inputArguments") final List<String> inputArguments,
                     @JsonProperty("uptime") final long uptime,
                     @JsonProperty("startTime") final long startTime) {
    this.name = name;
    this.vmName = vmName;
    this.vmVendor = vmVendor;
    this.vmVersion = vmVersion;
    this.specName = specName;
    this.specVendor = specVendor;
    this.specVersion = specVersion;
    this.inputArguments = inputArguments;
    this.uptime = uptime;
    this.startTime = startTime;
  }

  public RuntimeInfo(final Builder builder) {
    this.name = builder.name;
    this.vmName = builder.vmName;
    this.vmVendor = builder.vmVendor;
    this.vmVersion = builder.vmVersion;
    this.specName = builder.specName;
    this.specVendor = builder.specVendor;
    this.specVersion = builder.specVersion;
    this.inputArguments = builder.inputArguments;
    this.uptime = builder.uptime;
    this.startTime = builder.startTime;
  }

  public String getName() {
    return name;
  }

  public String getVmName() {
    return vmName;
  }

  public String getVmVendor() {
    return vmVendor;
  }

  public String getVmVersion() {
    return vmVersion;
  }

  public String getSpecName() {
    return specName;
  }

  public String getSpecVendor() {
    return specVendor;
  }

  public String getSpecVersion() {
    return specVersion;
  }

  public List<String> getInputArguments() {
    return inputArguments;
  }

  public long getUptime() {
    return uptime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RuntimeInfo that = (RuntimeInfo) o;

    if (startTime != that.startTime) {
      return false;
    }
    if (uptime != that.uptime) {
      return false;
    }
    if (inputArguments != null ? !inputArguments.equals(that.inputArguments)
                               : that.inputArguments != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (specName != null ? !specName.equals(that.specName) : that.specName != null) {
      return false;
    }
    if (specVendor != null ? !specVendor.equals(that.specVendor) : that.specVendor != null) {
      return false;
    }
    if (specVersion != null ? !specVersion.equals(that.specVersion) : that.specVersion != null) {
      return false;
    }
    if (vmName != null ? !vmName.equals(that.vmName) : that.vmName != null) {
      return false;
    }
    if (vmVendor != null ? !vmVendor.equals(that.vmVendor) : that.vmVendor != null) {
      return false;
    }
    if (vmVersion != null ? !vmVersion.equals(that.vmVersion) : that.vmVersion != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (vmName != null ? vmName.hashCode() : 0);
    result = 31 * result + (vmVendor != null ? vmVendor.hashCode() : 0);
    result = 31 * result + (vmVersion != null ? vmVersion.hashCode() : 0);
    result = 31 * result + (specName != null ? specName.hashCode() : 0);
    result = 31 * result + (specVendor != null ? specVendor.hashCode() : 0);
    result = 31 * result + (specVersion != null ? specVersion.hashCode() : 0);
    result = 31 * result + (inputArguments != null ? inputArguments.hashCode() : 0);
    result = 31 * result + (int) (uptime ^ (uptime >>> 32));
    result = 31 * result + (int) (startTime ^ (startTime >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "RuntimeInfo{" +
           "name='" + name + '\'' +
           ", vmName='" + vmName + '\'' +
           ", vmVendor='" + vmVendor + '\'' +
           ", vmVersion='" + vmVersion + '\'' +
           ", specName='" + specName + '\'' +
           ", specVendor='" + specVendor + '\'' +
           ", specVersion='" + specVersion + '\'' +
           ", inputArguments=" + inputArguments +
           ", uptime=" + uptime +
           ", startTime=" + startTime +
           '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String name;
    private String vmName;
    private String vmVendor;
    private String vmVersion;
    private String specName;
    private String specVendor;
    private String specVersion;
    private List<String> inputArguments;
    private long uptime;
    private long startTime;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setVmName(final String vmName) {
      this.vmName = vmName;
      return this;
    }

    public Builder setVmVendor(final String vmVendor) {
      this.vmVendor = vmVendor;
      return this;
    }

    public Builder setVmVersion(final String vmVersion) {
      this.vmVersion = vmVersion;
      return this;
    }

    public Builder setSpecName(final String specName) {
      this.specName = specName;
      return this;
    }

    public Builder setSpecVendor(final String specVendor) {
      this.specVendor = specVendor;
      return this;
    }

    public Builder setSpecVersion(final String specVersion) {
      this.specVersion = specVersion;
      return this;
    }

    public Builder setInputArguments(final List<String> inputArguments) {
      this.inputArguments = inputArguments;
      return this;
    }

    public Builder setUptime(final long uptime) {
      this.uptime = uptime;
      return this;
    }

    public Builder setStartTime(final long startTime) {
      this.startTime = startTime;
      return this;
    }

    public RuntimeInfo build() {
      return new RuntimeInfo(this);
    }
  }
}
