/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.descriptors;

import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.helios.common.Json;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

public class JobDescriptor extends Descriptor {

  private final String hash;
  private final String name;
  private final String version;
  private final String image;
  private final List<String> command;

  public JobDescriptor(@JsonProperty("hash") final String hash,
                       @JsonProperty("name") final String name,
                       @JsonProperty("version") final String version,
                       @JsonProperty("image") final String image,
                       @JsonProperty("command") final List<String> command) {
    this.hash = checkNotNull(hash);
    this.name = checkNotNull(name);
    this.version = checkNotNull(version);
    this.image = checkNotNull(image);
    this.command = checkNotNull(command);
  }

  public String getHash() {
    return hash;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getImage() {
    return image;
  }

  @JsonIgnore
  public String getId() {
    return name + ":" + version + ":" + hash;
  }

  public List<String> getCommand() {
    return command;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(final Object o) {

    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final JobDescriptor that = (JobDescriptor) o;

    if (command != null ? !command.equals(that.command) : that.command != null) {
      return false;
    }
    if (hash != null ? !hash.equals(that.hash) : that.hash != null) {
      return false;
    }
    if (image != null ? !image.equals(that.image) : that.image != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hash != null ? hash.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (image != null ? image.hashCode() : 0);
    result = 31 * result + (command != null ? command.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name)
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

    public JobDescriptor build() {
      final String hash;
      try {
        hash = hex(Json.sha1digest(p));
      } catch (IOException e) {
        throw propagate(e);
      }
      if (this.hash != null) {
        checkArgument(this.hash.equals(hash));
      }

      return new JobDescriptor(hash, p.name, p.version, p.image, p.command);
    }

    private String hex(final byte[] bytes) {
      return BaseEncoding.base16().lowerCase().encode(bytes);
    }
  }
}
