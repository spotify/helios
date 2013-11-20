/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ImageId extends Descriptor {

  private final String name;
  private final String tag;

  public ImageId(@JsonProperty("name") final String name,
                 @JsonProperty("tag") final String tag) {
    this.name = name;
    this.tag = tag;
  }

  public String getName() {
    return name;
  }

  public String getTag() {
    return tag;
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

    final ImageId that = (ImageId) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (tag != null ? !tag.equals(that.tag) : that.tag != null) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name)
        .add("tag", tag)
        .toString();
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (tag != null ? tag.hashCode() : 0);
    return result;
  }

  public static class Builder {

    private String name;
    private String tag;

    private Builder() {}

    public static Builder anImageDescriptor() { return new Builder();}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withTag(String tag) {
      this.tag = tag;
      return this;
    }

    public ImageId build() {
      return new ImageId(name, tag);
    }
  }
}
