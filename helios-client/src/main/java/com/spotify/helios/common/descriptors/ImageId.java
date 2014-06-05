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
