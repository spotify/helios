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

package com.spotify.helios.common.protocol;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;

public class CreateDeploymentGroupResponse {

  public enum Status {
    CREATED,
    NOT_MODIFIED,
    CONFLICT
  }

  private final Status status;

  public CreateDeploymentGroupResponse(@JsonProperty("status") final Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("status", status)
        .toString();
  }

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CreateDeploymentGroupResponse that = (CreateDeploymentGroupResponse) o;

    if (status != that.status) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return status != null ? status.hashCode() : 0;
  }
}
