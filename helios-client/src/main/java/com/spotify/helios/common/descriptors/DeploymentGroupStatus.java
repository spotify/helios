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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.EMPTY_LIST;

/**
 * The state of a deployment group.
 */
@JsonIgnoreProperties({"version"})
public class DeploymentGroupStatus extends Descriptor {

  public enum State {
    DETERMINING_HOSTS,
    ROLLING_OUT,
    FAILED,
    DONE,
  }

  private final DeploymentGroup deploymentGroup;
  private final State state;
  private final List<String> hosts;
  private final int index;
  private final String error;

  // TODO: ignore for JSON serialization
  private final int version;

  public static DeploymentGroupStatus of(final DeploymentGroup deploymentGroup,
                                         final State state) {
    return new DeploymentGroupStatus(deploymentGroup, state, EMPTY_LIST, 0, null, 0);
  }

  public static DeploymentGroupStatus of(final DeploymentGroup deploymentGroup,
                                         final State state, final List<String> hosts) {
    return new DeploymentGroupStatus(deploymentGroup, state, hosts, 0, null, 0);
  }

  public DeploymentGroupStatus(
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup,
      @JsonProperty("state") final State state, @JsonProperty("hosts") final List<String> hosts,
      @JsonProperty("index") final int index, @JsonProperty("error") final String error,
      @JsonProperty("version") final int version) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(state, "state");
    this.hosts = checkNotNull(hosts, "hosts");
    this.index = index;
    this.version = version;
    this.error = error;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setState(state)
        .setHosts(hosts)
        .setIndex(index)
        .setError(error)
        .setVersion(version);
  }

  private DeploymentGroupStatus(final Builder builder) {
    this.deploymentGroup = checkNotNull(builder.deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(builder.state, "state");
    this.hosts = checkNotNull(builder.hosts, "hosts");
    this.index = checkNotNull(builder.index, "index");
    this.error = builder.error;
    this.version = checkNotNull(builder.version, "version");
  }

  public DeploymentGroup getDeploymentGroup() {
    return deploymentGroup;
  }

  public State getState() {
    return state;
  }

  public List<String> getHosts() {
    return hosts;
  }

  public int getIndex() {
    return index;
  }

  public String getError() {
    return error;
  }

  public int getVersion() {
    return version;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeploymentGroupStatus that = (DeploymentGroupStatus) o;

    if (index != that.index) {
      return false;
    }
    if (version != that.version) {
      return false;
    }
    if (deploymentGroup != null ? !deploymentGroup.equals(that.deploymentGroup)
                                : that.deploymentGroup != null) {
      return false;
    }
    if (state != that.state) {
      return false;
    }
    if (hosts != null ? !hosts.equals(that.hosts) : that.hosts != null) {
      return false;
    }
    return !(error != null ? !error.equals(that.error) : that.error != null);

  }

  @Override
  public int hashCode() {
    int result = deploymentGroup != null ? deploymentGroup.hashCode() : 0;
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    result = 31 * result + index;
    result = 31 * result + (error != null ? error.hashCode() : 0);
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("deploymentGroup", deploymentGroup)
        .add("state", state)
        .add("hosts", hosts)
        .add("index", index)
        .add("error", error)
        .add("version", version)
        .toString();
  }

  public static class Builder {
    private DeploymentGroup deploymentGroup;
    private DeploymentGroupStatus.State state;
    private List<String> hosts;
    private int index;
    private String error;
    private int version;

    public Builder setDeploymentGroup(DeploymentGroup deploymentGroup) {
      this.deploymentGroup = deploymentGroup;
      return this;
    }

    public Builder setState(DeploymentGroupStatus.State state) {
      this.state = state;
      return this;
    }

    public Builder setHosts(List<String> hosts) {
      this.hosts = hosts;
      return this;
    }

    public Builder setIndex(int index) {
      this.index = index;
      return this;
    }

    public Builder setError(String error) {
      this.error = error;
      return this;
    }

    public Builder setVersion(int version) {
      this.version = version;
      return this;
    }

    public DeploymentGroupStatus build() {
      return new DeploymentGroupStatus(this);
    }
  }
}
