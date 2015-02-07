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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * A class that used to do something, but now is just effectively a placeholder in a Map
 * that is treated as a Set.  It is represented by the empty map in JSON.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServicePortParameters extends Descriptor {
  private final List<String> tags;

  public ServicePortParameters(@JsonProperty("tags") @Nullable final List<String> tags) {
    this.tags = tags;
  }

  public List<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServicePortParameters that = (ServicePortParameters) o;

    if (tags != null ? !tags.equals(that.tags) : that.tags != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return tags != null ? tags.hashCode() : 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("tags", tags)
        .toString();
  }
}
