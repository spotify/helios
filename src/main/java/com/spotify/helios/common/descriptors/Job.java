/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.spotify.helios.common.Hash.sha1digest;

public class Job extends Descriptor implements Comparable<Job> {

  private final JobId id;
  private final String image;
  private final List<String> command;

  public Job(@JsonProperty("id") final JobId id,
             @JsonProperty("image") final String image,
             @JsonProperty("command") final List<String> command) {
    this.id = checkNotNull(id);
    this.image = checkNotNull(image);
    this.command = checkNotNull(command);
  }

  public JobId getId() {
    return id;
  }


  public String getImage() {
    return image;
  }

  public List<String> getCommand() {
    return command;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int compareTo(final Job o) {
    return id.compareTo(o.getId());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Job job = (Job) o;

    if (command != null ? !command.equals(job.command) : job.command != null) {
      return false;
    }
    if (id != null ? !id.equals(job.id) : job.id != null) {
      return false;
    }
    if (image != null ? !image.equals(job.image) : job.image != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (image != null ? image.hashCode() : 0);
    result = 31 * result + (command != null ? command.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", id)
        .add("image", image)
        .add("command", command)
        .toString();
  }

  public static class Builder {

    private String hash;

    private static class Parameters {

      public String name;
      public String version;
      public String image;
      public List<String> command;
    }

    final Parameters p = new Parameters();

    public Builder setHash(final String hash) {
      this.hash = hash;
      return this;
    }

    public Builder setName(final String name) {
      p.name = name;
      return this;
    }

    public Builder setVersion(final String version) {
      p.version = version;
      return this;
    }

    public Builder setImage(final String image) {
      p.image = image;
      return this;
    }

    public Builder setCommand(final List<String> command) {
      p.command = command;
      return this;
    }

    public Job build() {
      final String configHash;
      try {
        configHash = hex(Json.sha1digest(p));
      } catch (IOException e) {
        throw propagate(e);
      }

      final String input = String.format("%s:%s:%s", p.name, p.version, configHash);
      final String hash = hex(sha1digest(input.getBytes(UTF_8)));

      if (this.hash != null) {
        checkArgument(this.hash.equals(hash));
      }

      final JobId id = new JobId(p.name, p.version, hash);

      return new Job(id, p.image, p.command);
    }

    private String hex(final byte[] bytes) {
      return BaseEncoding.base16().lowerCase().encode(bytes);
    }
  }
}
