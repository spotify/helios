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

import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
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

  public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toSeconds(5);
  public static final int DEFAULT_PARALLELISM = 1;

  private final long timeout;
  private final int parallelism;
  private final boolean migrate;
  private final boolean overlap;
  private final String token;
  private final boolean ignoreFailures;

  public RolloutOptions(@JsonProperty("timeout") final long timeout,
                        @JsonProperty("parallelism") final int parallelism,
                        @JsonProperty("migrate") final boolean migrate,
                        @JsonProperty("overlap") boolean overlap,
                        @JsonProperty("token") @Nullable String token,
                        @JsonProperty("ignoreFailures") boolean ignoreFailures) {
    this.timeout = timeout;
    this.parallelism = parallelism;
    this.migrate = migrate;
    this.overlap = overlap;
    this.token = Optional.fromNullable(token).or(EMPTY_TOKEN);
    this.ignoreFailures = ignoreFailures;
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
        .setIgnoreFailures(ignoreFailures);
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

  public boolean getIgnoreFailures() {
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
    if (ignoreFailures != that.ignoreFailures) {
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
    result = 31 * result + (ignoreFailures ? 1 : 0);
    return result;
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

  public RolloutOptions withFallback(final RolloutOptions other) {
    // TODO(nickplatt):
    // This should actually fallback to `other` for each property that hasn't been set explicitly.
    // Since RollingUpdateCommand provides default values, we don't have a good way to know whether
    // the flags were provided explicitly or whether they're just the defaults.
    return this;
  }

  public static class Builder {

    private long timeout;
    private int parallelism;
    private boolean migrate;
    private boolean overlap;
    private String token;
    private boolean ignoreFailures;

    public Builder() {
      this.timeout = DEFAULT_TIMEOUT;
      this.parallelism = DEFAULT_PARALLELISM;
      this.migrate = false;
      this.overlap = false;
      this.token = EMPTY_TOKEN;
      this.ignoreFailures = false;
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

    public Builder setIgnoreFailures(final boolean ignoreFailures) {
      this.ignoreFailures = ignoreFailures;
      return this;
    }

    public RolloutOptions build() {
      return new RolloutOptions(timeout, parallelism, migrate, overlap, token, ignoreFailures);
    }
  }
}
