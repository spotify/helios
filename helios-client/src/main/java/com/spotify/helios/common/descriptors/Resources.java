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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Sets the runtime constraints for a container.
 *
 * <pre>
 * {
 *   "memory"     : 10485760,
 *   "memorySwap" : 10485760,
 *   "cpuset"     : "0",
 *   "cpuShares"  : 512
 * }
 * </pre>
 */
public class Resources extends Descriptor {
  private final Long memory;
  private final Long memorySwap;
  private final Long cpuShares;
  private final String cpuset;

  public Resources(@JsonProperty("memory") final Long memory,
                   @JsonProperty("memorySwap") final Long memorySwap,
                   @JsonProperty("cpuShares") final Long cpuShares,
                   @JsonProperty("cpuset") final String cpuset) {
    this.memory = memory;
    this.memorySwap = memorySwap;
    this.cpuShares = cpuShares;
    this.cpuset = cpuset;
  }

  @Nullable
  public Long getMemory() {
    return memory;
  }

  @Nullable
  public Long getMemorySwap() {
    return memorySwap;
  }

  @Nullable
  public Long getCpuShares() {
    return cpuShares;
  }

  @Nullable
  public String getCpuset() {
    return cpuset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Resources that = (Resources) o;

    if (cpuShares != null ? !cpuShares.equals(that.cpuShares) : that.cpuShares != null) {
      return false;
    }
    if (cpuset != null ? !cpuset.equals(that.cpuset) : that.cpuset != null) {
      return false;
    }
    if (memory != null ? !memory.equals(that.memory) : that.memory != null) {
      return false;
    }
    if (memorySwap != null ? !memorySwap.equals(that.memorySwap) : that.memorySwap != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = memory != null ? memory.hashCode() : 0;
    result = 31 * result + (memorySwap != null ? memorySwap.hashCode() : 0);
    result = 31 * result + (cpuShares != null ? cpuShares.hashCode() : 0);
    result = 31 * result + (cpuset != null ? cpuset.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("memory", memory)
        .add("memorySwap", memorySwap)
        .add("cpuShares", cpuShares)
        .add("cpuset", cpuset)
        .toString();
  }
}
