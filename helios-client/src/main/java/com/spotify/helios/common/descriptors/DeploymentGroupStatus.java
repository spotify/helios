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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.EMPTY_LIST;

/**
 * The state of a deployment group.
 */
@JsonIgnoreProperties({"version"})
public class DeploymentGroupStatus extends Descriptor {

  public enum State {
    PLANNING_ROLLOUT,
    ROLLING_OUT,
    FAILED,
    DONE,
  }

  public enum DisplayState {
    ACTIVE,
    FAILED,
  }

  private final DeploymentGroup deploymentGroup;
  private final State state;
  private final DisplayState displayState;
  private final List<RolloutTask> rolloutTasks;
  private final List<String> hosts;
  private final int taskIndex;
  private final int hostIndex;
  private final int successfulIterations;
  private final int version;
  private final String error;

  private DeploymentGroupStatus(
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup,
      @JsonProperty("state") final State state,
      @JsonProperty("displayState") final DisplayState displayState,
      @JsonProperty("rolloutTasks") final List<RolloutTask> rolloutTasks,
      @JsonProperty("hosts") final List<String> hosts,
      @JsonProperty("taskIndex") final int taskIndex,
      @JsonProperty("hostIndex") final int hostIndex,
      @JsonProperty("successfulIterations") int successfulIterations,
      @JsonProperty("error") final String error,
      @JsonProperty("version") final int version) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(state, "state");
    this.displayState = displayState;
    this.rolloutTasks = checkNotNull(rolloutTasks, "rolloutTasks");
    this.hosts = hosts;
    this.taskIndex = taskIndex;
    this.hostIndex = hostIndex;
    this.successfulIterations = successfulIterations;
    this.error = error;
    this.version = version;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setState(state)
        .setRolloutTasks(rolloutTasks)
        .setTaskIndex(taskIndex)
        .setSuccessfulIterations(successfulIterations)
        .setError(error)
        .setVersion(version);
  }

  private DeploymentGroupStatus(final Builder builder) {
    this.deploymentGroup = checkNotNull(builder.deploymentGroup, "deploymentGroup");
    this.state = checkNotNull(builder.state, "state");
    this.displayState = displayState(state);
    this.rolloutTasks = checkNotNull(builder.rolloutTasks, "rolloutTasks");
    this.hosts = hosts(rolloutTasks);
    this.taskIndex = builder.taskIndex;
    this.hostIndex = taskIndex / 3;
    this.successfulIterations = builder.successfulIterations;
    this.error = builder.error;
    this.version = builder.version;
  }

  private static DisplayState displayState(final DeploymentGroupStatus.State state) {
    return (state.equals(State.FAILED)) ? DisplayState.FAILED : DisplayState.ACTIVE;
  }

  private static List<String> hosts(final List<RolloutTask> rolloutTasks) {
    final Set<String> uniqueHosts = Sets.newLinkedHashSet();
    for (RolloutTask task : rolloutTasks) {
      uniqueHosts.add(task.getTarget());
    }
    return Lists.newArrayList(uniqueHosts);
  }

  public DeploymentGroup getDeploymentGroup() {
    return deploymentGroup;
  }

  public State getState() {
    return state;
  }

  public DisplayState getDisplayState() {
    return displayState;
  }

  public List<RolloutTask> getRolloutTasks() {
    return rolloutTasks;
  }

  public List<String> getHosts() {
    return hosts;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public int getHostIndex() {
    return hostIndex;
  }

  public int getSuccessfulIterations() {
    return successfulIterations;
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

    if (taskIndex != that.taskIndex) {
      return false;
    }
    if (hostIndex != that.hostIndex) {
      return false;
    }
    if (version != that.version) {
      return false;
    }
    if (successfulIterations != that.successfulIterations) {
      return false;
    }
    if (deploymentGroup != null ? !deploymentGroup.equals(that.deploymentGroup)
                                : that.deploymentGroup != null) {
      return false;
    }
    if (state != that.state) {
      return false;
    }
    if (displayState != that.displayState) {
      return false;
    }
    if (rolloutTasks != null ? !rolloutTasks.equals(that.rolloutTasks)
                             : that.rolloutTasks != null) {
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
    result = 31 * result + (displayState != null ? displayState.hashCode() : 0);
    result = 31 * result + (rolloutTasks != null ? rolloutTasks.hashCode() : 0);
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    result = 31 * result + taskIndex;
    result = 31 * result + hostIndex;
    result = 31 * result + (error != null ? error.hashCode() : 0);
    result = 31 * result + version;
    result = 31 * result + successfulIterations;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("deploymentGroup", deploymentGroup)
        .add("state", state)
        .add("displayState", displayState)
        .add("rolloutTasks", rolloutTasks)
        .add("hosts", hosts)
        .add("taskIndex", taskIndex)
        .add("hostIndex", hostIndex)
        .add("error", error)
        .add("version", version)
        .add("successfulIterations", successfulIterations)
        .toString();
  }

  public static class Builder {
    private DeploymentGroup deploymentGroup;
    private DeploymentGroupStatus.State state;
    private List<RolloutTask> rolloutTasks = EMPTY_LIST;
    private int taskIndex;
    private int successfulIterations;
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

    public Builder setRolloutTasks(List<RolloutTask> rolloutTasks) {
      this.rolloutTasks = rolloutTasks;
      return this;
    }

    public Builder setTaskIndex(int taskIndex) {
      this.taskIndex = taskIndex;
      return this;
    }

    public Builder setSuccessfulIterations(int successfulIterations) {
      this.successfulIterations = successfulIterations;
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
