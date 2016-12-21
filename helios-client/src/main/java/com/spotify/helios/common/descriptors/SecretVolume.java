/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a secret volume response from secret-volume.
 *
 * <pre>
 *   {
 *     "ID": "containerId",
 *     "Source": "Talos",
 *     "Tags": {}
 *   }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SecretVolume extends Descriptor {

  protected final String id;
  protected final Secrets.Source source;
  protected final Map<String, List<String>> tags;

  public SecretVolume(
      @JsonProperty("ID") final String id,
      @JsonProperty("Source") final Secrets.Source source,
      @JsonProperty("Tags") final Map<String, List<String>> tags) {
    this.id = id;
    this.source = source;
    this.tags = tags;
  }

  public Secrets.Source getSource() {
    return this.source;
  }

  public String getId() {
    return this.id;
  }

  public Map<String, List<String>> getTags() {
    return this.tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SecretVolume that = (SecretVolume) o;

    return Objects.equals(this.id, that.id)
           && Objects.equals(this.source, that.source)
           && Objects.equals(this.tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, source, tags);
  }

  @Override
  public String toString() {
    return "Secrets{"
           + "source=" + source
           + ", id=" + id
           + ", tags=" + tags
           + "} " + super.toString();
  }
}
