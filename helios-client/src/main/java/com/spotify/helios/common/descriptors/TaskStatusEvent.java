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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents something that has happened to a Task.
 *
 * A typical JSON representation of a task might be:
 * <pre>
 * {
 *   "status" : { #... see definition of TaskStatus },
 *   "timestamp" : 1410308461448,
 *   "host": "myhost"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskStatusEvent {
  private final TaskStatus status;
  private final long timestamp;
  private final String host;

  /**
   * Constructor.
   *
   * @param status  The status of the task at the point of the event.
   * @param timestamp The timestamp of the event.
   * @param host The host on which the event occurred.
   */
  public TaskStatusEvent(@JsonProperty("status") final TaskStatus status,
                         @JsonProperty("timestamp") final long timestamp,
                         @JsonProperty("host") final String host) {
    this.status = status;
    this.timestamp = timestamp;
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(TaskStatusEvent.class)
        .add("timestamp", timestamp)
        .add("host", host)
        .add("status", status)
        .toString();
  }
}
