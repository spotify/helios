/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HostInfo extends Descriptor {

  private final String hostname;
  private final String uname;
  private final String architecture;
  private final String osName;
  private final String osVersion;
  private final int cpus;
  private final double loadAvg;
  private final long memoryTotalBytes;
  private final long memoryFreeBytes;
  private final long swapTotalBytes;
  private final long swapFreeBytes;
  private final DockerVersion dockerVersion;

  public HostInfo(@JsonProperty("hostname") final String hostname,
                  @JsonProperty("uname") final String uname,
                  @JsonProperty("architecture") final String architecture,
                  @JsonProperty("osName") final String osName,
                  @JsonProperty("osVersion") final String osVersion,
                  @JsonProperty("cpus") final int cpus,
                  @JsonProperty("loadAvg") final double loadAvg,
                  @JsonProperty("memoryTotalBytes") final long memoryTotalBytes,
                  @JsonProperty("memoryFreeBytes") final long memoryFreeBytes,
                  @JsonProperty("swapTotalBytes") final long swapTotalBytes,
                  @JsonProperty("swapFreeBytes") final long swapFreeBytes,
                  @JsonProperty("dockerVersion") final DockerVersion dockerVersion) {
    this.hostname = hostname;
    this.uname = uname;
    this.architecture = architecture;
    this.osName = osName;
    this.osVersion = osVersion;
    this.cpus = cpus;
    this.loadAvg = loadAvg;
    this.memoryTotalBytes = memoryTotalBytes;
    this.memoryFreeBytes = memoryFreeBytes;
    this.swapTotalBytes = swapTotalBytes;
    this.swapFreeBytes = swapFreeBytes;
    this.dockerVersion = dockerVersion;
  }

  public HostInfo(final Builder builder) {
    this.hostname = builder.hostname;
    this.uname = builder.uname;
    this.architecture = builder.architecture;
    this.osName = builder.osName;
    this.osVersion = builder.osVersion;
    this.cpus = builder.cpus;
    this.loadAvg = builder.loadAvg;
    this.memoryTotalBytes = builder.memoryTotalBytes;
    this.memoryFreeBytes = builder.memoryFreeBytes;
    this.swapTotalBytes = builder.swapTotalBytes;
    this.swapFreeBytes = builder.swapFreeBytes;
    this.dockerVersion = builder.dockerVersion;
  }

  public String getHostname() {
    return hostname;
  }

  public String getUname() {
    return uname;
  }

  public String getArchitecture() {
    return architecture;
  }

  public String getOsName() {
    return osName;
  }

  public String getOsVersion() {
    return osVersion;
  }

  public int getCpus() {
    return cpus;
  }

  public double getLoadAvg() {
    return loadAvg;
  }

  public long getMemoryTotalBytes() {
    return memoryTotalBytes;
  }

  public long getMemoryFreeBytes() {
    return memoryFreeBytes;
  }

  public long getSwapTotalBytes() {
    return swapTotalBytes;
  }

  public long getSwapFreeBytes() {
    return swapFreeBytes;
  }

  public DockerVersion getDockerVersion() {
    return dockerVersion;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String hostname;
    private String uname;
    private String architecture;
    private String osName;
    private String osVersion;
    private int cpus;
    private double loadAvg;
    private long memoryTotalBytes;
    private long memoryFreeBytes;
    private long swapTotalBytes;
    private long swapFreeBytes;
    private DockerVersion dockerVersion;

    public Builder setHostname(final String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setUname(final String uname) {
      this.uname = uname;
      return this;
    }

    public Builder setArchitecture(final String architecture) {
      this.architecture = architecture;
      return this;
    }

    public Builder setOsName(final String osName) {
      this.osName = osName;
      return this;
    }

    public Builder setOsVersion(final String osVersion) {
      this.osVersion = osVersion;
      return this;
    }

    public Builder setCpus(final int cpus) {
      this.cpus = cpus;
      return this;
    }

    public Builder setLoadAvg(final double loadAvg) {
      this.loadAvg = loadAvg;
      return this;
    }

    public Builder setMemoryTotalBytes(final long memoryTotalBytes) {
      this.memoryTotalBytes = memoryTotalBytes;
      return this;
    }

    public Builder setMemoryFreeBytes(final long memoryFreeBytes) {
      this.memoryFreeBytes = memoryFreeBytes;
      return this;
    }

    public Builder setSwapTotalBytes(final long swapTotalBytes) {
      this.swapTotalBytes = swapTotalBytes;
      return this;
    }

    public Builder setSwapFreeBytes(final long swapFreeBytes) {
      this.swapFreeBytes = swapFreeBytes;
      return this;
    }

    public Builder setDockerVersion(final DockerVersion dockerVersion) {
      this.dockerVersion = dockerVersion;
      return this;
    }

    public HostInfo build() {
      return new HostInfo(this);
    }
  }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("hostname", hostname)
        .add("uname", uname)
        .add("architecture", architecture)
        .add("osName", osName)
        .add("osVersion", osVersion)
        .add("cpus", cpus)
        .add("loadAvg", loadAvg)
        .add("memoryTotalBytes", memoryTotalBytes)
        .add("memoryFreeBytes", memoryFreeBytes)
        .add("swapTotalBytes", swapTotalBytes)
        .add("swapFreeBytes", swapFreeBytes)
        .toString();
  }
}
