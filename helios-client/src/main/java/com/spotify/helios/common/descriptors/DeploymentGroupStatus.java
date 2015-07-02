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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The state of a deployment group.
 */
public class DeploymentGroupStatus extends Descriptor {

  public enum State {
    QUEUED,
    ROLLING_OUT,
    FAILED,
    DONE
  }

  private final DeploymentGroup deploymentGroup;
  private final State state;
  private final List<String> hosts;
  private final int index;
  private final int version;

  public DeploymentGroupStatus(
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup,
      @JsonProperty("state") final State state, @JsonProperty("hosts") final List<String> hosts,
      @JsonProperty("index") final int index, @JsonProperty("version") final int version) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(state, "state");
    this.hosts = checkNotNull(hosts, "hosts");
    this.index = index;
    this.version = version;
  }

  public Builder asBuilder() {
    return newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setState(state)
        .setHosts(hosts)
        .setIndex(index)
        .setVersion(version);
  }

  private DeploymentGroupStatus(final Builder builder) {
    this.deploymentGroup = checkNotNull(builder.deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(builder.state, "state");
    this.hosts = checkNotNull(builder.hosts, "hosts");
    this.index = checkNotNull(builder.index, "index");
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

  public int getVersion() {
    return version;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeploymentGroupStatus that = (DeploymentGroupStatus) o;

    if (version != that.version) {
      return false;
    }
    if (index != that.index) {
      return false;
    }
    if (!deploymentGroup.equals(that.deploymentGroup)) {
      return false;
    }
    if (state != that.state) {
      return false;
    }
    return hosts.equals(that.hosts);

  }

  @Override
  public int hashCode() {
    int result = deploymentGroup.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + hosts.hashCode();
    result = 31 * result + index;
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
        .add("version", version)
        .toString();
  }

  public static class Builder {
    private DeploymentGroup deploymentGroup;
    private DeploymentGroupStatus.State state;
    private List<String> hosts;
    private int index;
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

    public Builder setVersion(int version) {
      this.version = version;
      return this;
    }

    public DeploymentGroupStatus build() {
      return new DeploymentGroupStatus(this);
    }
  }
}
