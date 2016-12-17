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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Stores metadata for a service port. Currently, only tags are supported.
 *
 * <p>The tags can be used in service registration plugins trough ServiceRegistration.Endpoint.
 *
 * <p>An example expression of a Helios job with service port metadata might be:
 * <pre>
 * {
 *   "ports" : {
 *     "http" : {
 *       "externalPort" : 8060,
 *       "internalPort" : 8080,
 *       "protocol" : "tcp"
 *     }
 *   },
 *   "registration" : {
 *     "service/http" : {
 *       "ports" : {
 *         "http" : {
 *           "tags" : ["tag-1", "tag-2"]
 *         }
 *       }
 *     }
 *   }
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServicePortParameters extends Descriptor {
  private final List<String> tags;

  public ServicePortParameters(@JsonProperty("tags") @Nullable final List<String> tags) {
    this.tags = tags;
  }

  public List<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final ServicePortParameters that = (ServicePortParameters) obj;

    if (tags != null ? !tags.equals(that.tags) : that.tags != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return tags != null ? tags.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "ServicePortParameters{"
           + "tags=" + tags
           + '}';
  }
}
