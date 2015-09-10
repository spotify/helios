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

package com.spotify.helios.testing.descriptors;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Represents an event that will be collected for logging purposes.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({"step", "timestamp", "duration", "success", "testClassName", "testName"})
public class TemporaryJobEvent {
  private final double timestamp;
  private final double duration;
  private final String testClassName;
  private final String testName;
  private final String step;
  private final Boolean success;
  private final Map<String, Object> tags;

  public TemporaryJobEvent(@JsonProperty("timestamp") final double timestamp,
                           @JsonProperty("duration") final double duration,
                           @JsonProperty("testClassName") final String testClassName,
                           @JsonProperty("testName") final String testName,
                           @JsonProperty("step") final String step,
                           @Nullable @JsonProperty("success") final Boolean success,
                           @Nullable @JsonProperty("tags") final Map<String, Object> tags) {
    this.timestamp = timestamp;
    this.duration = duration;
    this.testClassName = testClassName;
    this.testName = testName;
    this.step = step;
    this.success = success;

    if (tags != null) {
      this.tags = ImmutableMap.copyOf(tags);
    } else {
      this.tags = ImmutableMap.of();
    }
  }

  public double getDuration() {
    return duration;
  }

  public double getTimestamp() {
    return timestamp;
  }

  public String getTestClassName() {
    return testClassName;
  }

  public String getTestName() {
    return testName;
  }

  public String getStep() {
    return step;
  }

  public Boolean isSuccess() {
    return success;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("duration", duration)
        .add("testClassName", testClassName)
        .add("testName", testName)
        .add("step", step)
        .add("success", success)
        .add("tags", tags)
        .toString();
  }
}
