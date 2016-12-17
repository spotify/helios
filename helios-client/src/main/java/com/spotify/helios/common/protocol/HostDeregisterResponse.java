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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;

public class HostDeregisterResponse {

  public enum Status {
    OK, NOT_FOUND, JOBS_STILL_DEPLOYED
  }

  private final Status status;
  private final String host;

  public HostDeregisterResponse(@JsonProperty("status") Status status,
                                @JsonProperty("host") String host) {
    this.status = status;
    this.host = host;
  }

  public Status getStatus() {
    return status;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return "HostDeregisterResponse{"
           + "status=" + status
           + ", host='" + host + '\''
           + '}';
  }

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }
}
