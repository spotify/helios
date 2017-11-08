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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a deployment group's rollout options.
 *
 * <p>An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "migrate": false,
 *   "parallelism": 2,
 *   "timeout": 1000,
 *   "overlap": true,
 *   "token": "insecure-access-token",
 *   "ignoreFailures": false
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RolloutOptions {

  private static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toSeconds(5);
  private static final int DEFAULT_PARALLELISM = 1;
  static final boolean DEFAULT_MIGRATE = false;
  private static final boolean DEFAULT_OVERLAP = false;
  static final String DEFAULT_TOKEN = Job.EMPTY_TOKEN;
  static final boolean DEFAULT_IGNORE_FAILURES = false;

  private static final RolloutOptions DEFAULT = RolloutOptions.newBuilder()
      .setTimeout(DEFAULT_TIMEOUT)
      .setParallelism(DEFAULT_PARALLELISM)
      .setMigrate(DEFAULT_MIGRATE)
      .setOverlap(DEFAULT_OVERLAP)
      .setToken(DEFAULT_TOKEN)
      .setIgnoreFailures(DEFAULT_IGNORE_FAILURES)
      .build();

  private final Long timeout;
  private final Integer parallelism;
  private final Boolean migrate;
  private final Boolean overlap;
  private final String token;
  private final Boolean ignoreFailures;

  private RolloutOptions(@JsonProperty("timeout") @Nullable final Long timeout,
                         @JsonProperty("parallelism") @Nullable final Integer parallelism,
                         @JsonProperty("migrate") @Nullable final Boolean migrate,
                         @JsonProperty("overlap") @Nullable final Boolean overlap,
                         @JsonProperty("token") @Nullable final String token,
                         @JsonProperty("ignoreFailures") @Nullable final Boolean ignoreFailures) {
    this.timeout = timeout;
    this.parallelism = parallelism;
    this.migrate = migrate;
    this.overlap = overlap;
    this.token = token;
    this.ignoreFailures = ignoreFailures;
  }

  public static RolloutOptions getDefault() {
    return DEFAULT;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder()
        .setTimeout(timeout)
        .setParallelism(parallelism)
        .setMigrate(migrate)
        .setOverlap(overlap)
        .setToken(token)
        .setIgnoreFailures(ignoreFailures);
  }

  /**
   * Return a new RolloutOptions instance by merging this instance with another one.
   * @throws NullPointerException if any attribute in both instances are null.
   */
  public RolloutOptions withFallback(final RolloutOptions that) {
    return RolloutOptions.newBuilder()
        .setTimeout(firstNonNull(timeout, that.timeout))
        .setParallelism(firstNonNull(parallelism, that.parallelism))
        .setMigrate(firstNonNull(migrate, that.migrate))
        .setOverlap(firstNonNull(overlap, that.overlap))
        .setToken(firstNonNull(token, that.token))
        .setIgnoreFailures(firstNonNull(ignoreFailures, that.ignoreFailures))
        .build();
  }

  @Nullable
  public Long getTimeout() {
    return timeout;
  }

  @Nullable
  public Integer getParallelism() {
    return parallelism;
  }

  @Nullable
  public Boolean getMigrate() {
    return migrate;
  }

  @Nullable
  public Boolean getOverlap() {
    return overlap;
  }

  @Nullable
  public String getToken() {
    return token;
  }

  @Nullable
  public Boolean getIgnoreFailures() {
    return ignoreFailures;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final RolloutOptions that = (RolloutOptions) obj;

    return Objects.equals(this.migrate, that.migrate)
           && Objects.equals(this.parallelism, that.parallelism)
           && Objects.equals(this.timeout, that.timeout)
           && Objects.equals(this.overlap, that.overlap)
           && Objects.equals(this.token, that.token)
           && Objects.equals(this.ignoreFailures, that.ignoreFailures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeout, parallelism, migrate, overlap, token, ignoreFailures);
  }

  @Override
  public String toString() {
    return "RolloutOptions{"
           + "timeout=" + timeout
           + ", parallelism=" + parallelism
           + ", migrate=" + migrate
           + ", overlap=" + overlap
           + ", token=" + token
           + ", ignoreFailures=" + ignoreFailures
           + '}';
  }

  public static class Builder {

    private Long timeout;
    private Integer parallelism;
    private Boolean migrate;
    private Boolean overlap;
    private String token;
    private Boolean ignoreFailures;

    public Builder() { }

    public Builder setTimeout(final Long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder setParallelism(final Integer parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder setMigrate(final Boolean migrate) {
      this.migrate = migrate;
      return this;
    }

    public Builder setOverlap(final Boolean overlap) {
      this.overlap = overlap;
      return this;
    }

    public Builder setToken(final String token) {
      this.token = token;
      return this;
    }

    public Builder setIgnoreFailures(final Boolean ignoreFailures) {
      this.ignoreFailures = ignoreFailures;
      return this;
    }

    public RolloutOptions build() {
      return new RolloutOptions(timeout, parallelism, migrate, overlap, token, ignoreFailures);
    }
  }
}
