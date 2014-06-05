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
import com.spotify.helios.common.descriptors.JobId;

public class SetGoalResponse {

  public enum Status {
    OK,
    JOB_NOT_FOUND,
    HOST_NOT_FOUND,
    JOB_NOT_DEPLOYED,
    ID_MISMATCH,
    INVALID_ID
  }

  private final Status status;
  private final JobId job;
  private final String host;

  public SetGoalResponse(@JsonProperty("status") Status status,
                         @JsonProperty("host") String host,
                         @JsonProperty("job") JobId job) {
    this.status = status;
    this.job = job;
    this.host = host;
  }

  public Status getStatus() {
    return status;
  }

  public JobId getJob() {
    return job;
  }

  public String getHost() {
    return host;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper("JobDeployResponse")
        .add("status", status)
        .add("host", host)
        .add("job", job)
        .toString();
  }
}
