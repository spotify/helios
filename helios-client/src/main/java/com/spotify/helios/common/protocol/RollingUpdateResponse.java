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

package com.spotify.helios.common.protocol;

import com.spotify.helios.common.Json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RollingUpdateResponse {

  public enum Status {
    OK,
    JOB_NOT_FOUND,
    DEPLOYMENT_GROUP_NOT_FOUND
  }

  private final Status status;

  public RollingUpdateResponse(@JsonProperty("status") Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "RollingUpdateResponse{" +
           "status=" + status +
           '}';
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

    final RollingUpdateResponse response = (RollingUpdateResponse) o;

    if (status != response.status) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return status != null ? status.hashCode() : 0;
  }
}
