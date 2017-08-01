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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

/**
 * Uniquely Identifies Jobs
 *
 * <p>Has a string representation in JSON of:
 * <pre>
 * "name:version:hashvalue"
 * </pre>
 *
 * <p>The hash value is so that if you are talking to multiple clusters, and the job definitions
 * are slightly different, even though they have the same name and version, they will still
 * be uniquely identifiable.  This is most important when executing commands against multiple
 * clusters.
 *
 * <p>Many endpoints taking JobId can take an abbreviated JobId.  That is, one without a the final
 * colon and hash value.
 */
@JsonSerialize
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobId extends Descriptor implements Comparable<JobId> {

  private final String name;
  private final String version;
  private final String hash;

  /**
   * Create a fully qualified job id with name, version and hash.
   *
   * @param name    The name of the job.
   * @param version The version of the job.
   * @param hash    The hash of the job.
   */
  public JobId(final String name,
               final String version,
               final String hash) {
    this.name = name;
    this.version = version;
    this.hash = hash;
  }

  /**
   * Create a new job id with a specific name and version.
   *
   * @param name    The name of the job.
   * @param version the version of the job.
   */
  public JobId(final String name,
               final String version) {
    this.name = name;
    this.version = version;
    this.hash = null;
  }

  /**
   * Private constructor for use by jackson.
   *
   * @param id The ID of the job.
   */
  @JsonCreator
  private JobId(final String id) {
    final String[] parts = id.split(":");
    if (parts.length != 2 && parts.length != 3) {
      throw new IllegalArgumentException("Invalid Job id: " + id);
    }
    this.name = parts[0];
    this.version = parts[1];
    this.hash = (parts.length == 3) ? parts[2] : null;
  }

  /**
   * Private constructor for use by {@link #parse(String)}
   *
   * @param name The name of the job.
   */
  private JobId(final String name, boolean ignored) {
    checkArgument(!checkNotNull(name, "name is null").isEmpty(), "name is empty");
    this.name = name;
    this.version = null;
    this.hash = null;
  }

  /**
   * Parse a job id string.
   *
   * <p>This parsing method can be used when input is trusted, i.e. failing to parse it indicates
   * programming error and not bad input.
   *
   * @param id A string representation of the job ID.
   *
   * @return The JobId object.
   *
   * @see #parse(String)
   */
  public static JobId fromString(final String id) {
    try {
      return parse(id);
    } catch (JobIdParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Parse a job id string.
   *
   * <p>This parsing method can be used when input is not know to be correct. I.e. when parsing a
   * job id supplied by the user in the cli or when parsing a request in the master rest
   * interface.
   *
   * @param id A string representation of the job ID.
   *
   * @return The JobId object.
   *
   * @throws JobIdParseException If the ID cannot be parsed.
   * @see #fromString(String)
   */
  public static JobId parse(final String id) throws JobIdParseException {
    final String[] parts = id.split(":");
    switch (parts.length) {
      case 1:
        return new JobId(parts[0], true);
      case 2:
        return new JobId(parts[0], parts[1]);
      case 3:
        return new JobId(parts[0], parts[1], parts[2]);
      default:
        throw new JobIdParseException("Invalid Job id: " + id);
    }
  }

  @Override
  @JsonValue
  public String toString() {
    if (hash == null) {
      return name + ":" + version;
    } else {
      return name + ":" + version + ":" + hash;
    }
  }

  public String toShortString() {
    if (hash == null) {
      return name + ":" + version;
    } else {
      return name + ":" + version + ":" + hash.substring(0, 7);
    }
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getHash() {
    return hash;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final JobId jobId = (JobId) obj;

    if (hash != null ? !hash.equals(jobId.hash) : jobId.hash != null) {
      return false;
    }
    if (name != null ? !name.equals(jobId.name) : jobId.name != null) {
      return false;
    }
    if (version != null ? !version.equals(jobId.version) : jobId.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (hash != null ? hash.hashCode() : 0);
    return result;
  }

  @Override
  public int compareTo(final JobId jobId) {
    return ComparisonChain.start()
        .compare(name, jobId.name)
        .compare(version, jobId.version)
        .compare(hash, jobId.hash, Ordering.natural().nullsFirst())
        .result();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public boolean isFullyQualified() {
    return name != null && version != null && hash != null && hash.length() == 40;
  }

  public static class Builder {

    private String name;
    private String version;
    private String hash;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setVersion(final String version) {
      this.version = version;
      return this;
    }

    public Builder setHash(final String hash) {
      this.hash = hash;
      return this;
    }

    public JobId build() {
      if (hash == null) {
        return new JobId(name, version);
      } else {
        return new JobId(name, version, hash);
      }
    }
  }
}
