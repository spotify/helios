/*
 * Copyright (c) 2015 Spotify AB.
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

import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Represents a Helios deployment group.
 *
 * An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "name":"foo-group",
 *   "hostSelectors":[
 *     {
 *       "label":"foo",
 *       "operator":"EQUALS"
 *       "operand":"bar",
 *     },
 *     {
 *       "label":"baz",
 *       "operator":"EQUALS"
 *       "operand":"qux",
 *     }
 *   ]
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeploymentGroup extends Descriptor {

  public static final String EMPTY_NAME = "";

  private final String name;
  private final List<HostSelector> hostSelectors;

  /**
   * Create a Job.
   *
   * @param name The docker name to use.
   * @param hostSelectors The selectors that determine which agents are part of the deployment
   *                       group.
   */
  public DeploymentGroup(
      @JsonProperty("name") final String name,
      @JsonProperty("hostSelectors") final List<HostSelector> hostSelectors) {
    this.name = name;
    this.hostSelectors = hostSelectors;
  }

  public String getName() {
    return name;
  }

  public List<HostSelector> getHostSelectors() {
    return hostSelectors;
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

    final DeploymentGroup that = (DeploymentGroup) o;

    if (hostSelectors != null ? !hostSelectors.equals(that.hostSelectors)
                               : that.hostSelectors != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (hostSelectors != null ? hostSelectors.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentGroup{" +
           "name='" + name + '\'' +
           ", hostSelectors=" + hostSelectors +
           '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    return builder.setName(name)
        .setHostSelectors(hostSelectors);
  }

  public static class Builder implements Cloneable {

    private final Parameters p;

    private Builder() {
      this.p = new Parameters();
    }

    private static class Parameters implements Cloneable {

      public String name;
      public List<HostSelector> hostSelectors;

      private Parameters() {
        this.name = EMPTY_NAME;
        this.hostSelectors = emptyList();
      }
    }

    public String getName() {
      return p.name;
    }

    public Builder setName(final String name) {
      p.name = name;
      return this;
    }

    public List<HostSelector> getHostSelectors() {
      return p.hostSelectors;
    }

    public Builder setHostSelectors(final List<HostSelector> hostSelectors) {
      p.hostSelectors = Lists.newArrayList(hostSelectors);
      return this;
    }

    public DeploymentGroup build() {
      return new DeploymentGroup(p.name, p.hostSelectors);
    }
  }

}
