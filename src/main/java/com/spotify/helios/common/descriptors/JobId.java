/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JobId implements Comparable<JobId> {

  private final String name;
  private final String version;
  private final String hash;

  public JobId(final String name,
               final String version,
               final String hash) {
    checkArgument(!checkNotNull(name, "name is null").isEmpty(), "name is empty");
    checkArgument(!checkNotNull(version, "version is null").isEmpty(), "version is empty");
    checkArgument(!checkNotNull(hash, "hash is null").isEmpty(), "hash is empty");
    this.name = name;
    this.version = version;
    this.hash = hash;
  }

  public JobId(final String name,
               final String version) {
    checkArgument(!checkNotNull(name, "name is null").isEmpty(), "name is empty");
    checkArgument(!checkNotNull(version, "version is null").isEmpty(), "version is empty");
    this.name = name;
    this.version = version;
    this.hash = null;
  }

  /**
   * Parse a job id string.
   *
   * This parsing method can be used when input is trusted, i.e. failing to parse it indicates
   * programming error and not bad input.
   * @see #parse(String)
   */
  @JsonCreator
  public static JobId fromString(final String id) {
    try {
      return parse(id);
    } catch (JobIdParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static final Function<String, JobId> FROM_STRING = new Function<String, JobId>() {
    @Nullable
    @Override
    public JobId apply(@Nullable final String s) {
      return fromString(s);
    }
  };

  /**
   * Parse a job id string.
   *
   * This parsing method can be used when input is not know to be correct. I.e. when parsing a job
   * id supplied by the user in the cli or when parsing a request in the master rest interface.
   * @see #fromString(String)
   */
  public static JobId parse(final String id) throws JobIdParseException {
    final String[] parts = id.split(":");
    switch (parts.length) {
      case 2:
        return new JobId(parts[0], parts[1]);
      case 3:
        return new JobId(parts[0], parts[1], parts[2]);
      default:
        throw new JobIdParseException("Invalid Job id: " + id);
    }
  }

  @JsonValue
  public String toString() {
    return name + ":" + version + ":" + hash;
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final JobId jobId = (JobId) o;

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
  public int compareTo(final JobId o) {
    return toString().compareTo(o.toString());
  }
}
