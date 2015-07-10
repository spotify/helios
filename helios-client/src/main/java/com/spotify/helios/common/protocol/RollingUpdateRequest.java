/*
 * Copyright (c) 2015 Spotify AB.
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
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;

import static com.google.common.base.Preconditions.checkNotNull;

public class RollingUpdateRequest {

  private final JobId job;
  private final RolloutOptions rolloutOptions;

  public RollingUpdateRequest(@JsonProperty("job") final JobId job,
                              @JsonProperty("rolloutOptions") final RolloutOptions rolloutOptions) {
    this.job = checkNotNull(job);
    this.rolloutOptions = rolloutOptions;
  }

  public JobId getJob() {
    return job;
  }

  public RolloutOptions getRolloutOptions() {
    return rolloutOptions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("job", job)
        .add("rolloutOptions", rolloutOptions)
        .toString();
  }

  public String toJsonString() {
    return Json.asStringUnchecked(this);
  }
}
