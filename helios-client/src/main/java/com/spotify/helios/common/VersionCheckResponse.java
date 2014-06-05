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

package com.spotify.helios.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.VersionCompatibility.Status;

public class VersionCheckResponse {
  private final PomVersion serverVersion;
  private final Status status;
  private final String recommendedVersion;

  public VersionCheckResponse(@JsonProperty("status") VersionCompatibility.Status status,
                              @JsonProperty("version") PomVersion serverVersion,
                              @JsonProperty("recommendedVersion") String recommendedVersion) {
    this.status = status;
    this.serverVersion = serverVersion;
    this.recommendedVersion = recommendedVersion;
  }

  public PomVersion getServerVersion() {
    return serverVersion;
  }

  public Status getStatus() {
    return status;
  }

  public String getRecommendedVersion() {
    return recommendedVersion;
  }
}
