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

package com.spotify.helios.common.descriptors;

import com.google.common.base.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;

/**
 * Represents a deployment group's rollout options.
 *
 * An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "name": "foo-group",
 *   "job": "foo:0.1.0",
 *   "hostSelectors": [
 *     {
 *       "label": "foo",
 *       "operator": "EQUALS"
 *       "operand": "bar",
 *     },
 *     {
 *       "label": "baz",
 *       "operator": "EQUALS"
 *       "operand": "qux",
 *     }
 *   ],
 *   "rolloutOptions": {
 *     "migrate": false,
 *     "parallelism": 2,
 *     "timeout": 1000,
 *     "overlap": true,
 *     "token": "insecure-access-token",
 *     "failureThreshold": 0.5
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RolloutOptions {

  public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toSeconds(5);
  public static final int DEFAULT_PARALLELISM = 1;
  public static final float DEFAULT_FAILURE_THRESHOLD = 0;

  private final long timeout;
  private final int parallelism;
  private final boolean migrate;
  private final boolean overlap;
  private final String token;
  private final float failureThreshold;

  public RolloutOptions(
      @JsonProperty("timeout") final long timeout,
      @JsonProperty("parallelism") final int parallelism,
      @JsonProperty("migrate") final boolean migrate,
      @JsonProperty("overlap") boolean overlap,
      @JsonProperty("token") @Nullable String token,
      @JsonProperty("failureThreshold") @Nullable final Float failureThreshold) {
    this.timeout = timeout;
    this.parallelism = parallelism;
    this.migrate = migrate;
    this.overlap = overlap;
    this.token = Optional.fromNullable(token).or(EMPTY_TOKEN);
    this.failureThreshold = Optional.fromNullable(failureThreshold).or(DEFAULT_FAILURE_THRESHOLD);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setTimeout(timeout)
        .setParallelism(parallelism)
        .setMigrate(migrate)
        .setToken(token)
        .setFailureThreshold(failureThreshold);
  }

  public long getTimeout() {
    return timeout;
  }

  public int getParallelism() {
    return parallelism;
  }

  public boolean getMigrate() {
    return migrate;
  }

  public boolean getOverlap() {
    return overlap;
  }

  public String getToken() {
    return token;
  }

  public float getFailureThreshold() {
    return failureThreshold;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RolloutOptions that = (RolloutOptions) o;

    if (migrate != that.migrate) {
      return false;
    }
    if (parallelism != that.parallelism) {
      return false;
    }
    if (timeout != that.timeout) {
      return false;
    }
    if (overlap != that.overlap) {
      return false;
    }
    if (token != null ? !token.equals(that.token) : that.token != null) {
      return false;
    }
    if (failureThreshold != that.failureThreshold) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (timeout ^ (timeout >>> 32));
    result = 31 * result + parallelism;
    result = 31 * result + (migrate ? 1 : 0);
    result = 31 * result + (overlap ? 1 : 0);
    result = 31 * result + (token != null ? token.hashCode() : 0);
    result = 31 * result + (failureThreshold != +0.0f ? Float.floatToIntBits(failureThreshold) : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RolloutOptions{" +
           "timeout=" + timeout +
           ", parallelism=" + parallelism +
           ", migrate=" + migrate +
           ", overlap=" + overlap +
           ", token=" + token +
           ", failure threshold=" + failureThreshold +
           '}';
  }

  public static class Builder {

    private long timeout;
    private int parallelism;
    private boolean migrate;
    private boolean overlap;
    private String token;
    private Float failureThreshold;

    public Builder() {
      this.timeout = DEFAULT_TIMEOUT;
      this.parallelism = DEFAULT_PARALLELISM;
      this.migrate = false;
      this.overlap = false;
      this.token = EMPTY_TOKEN;
      this.failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    }


    public Builder setTimeout(final long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder setParallelism(final int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder setMigrate(final boolean migrate) {
      this.migrate = migrate;
      return this;
    }

    public Builder setOverlap(final boolean overlap) {
      this.overlap = overlap;
      return this;
    }

    public Builder setToken(final String token) {
      this.token = token;
      return this;
    }

    public Builder setFailureThreshold(final Float failureThreshold) {
      this.failureThreshold = failureThreshold;
      return this;
    }

    public RolloutOptions build() {
      return new RolloutOptions(timeout, parallelism, migrate, overlap, token, failureThreshold);
    }
  }
}
