/*
 * Copyright (c) 2015 Spotify AB.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DeploymentGroupTasks extends Descriptor {

  private final List<RolloutTask> rolloutTasks;
  private final int taskIndex;
  private final DeploymentGroup deploymentGroup;

  private DeploymentGroupTasks(
      @JsonProperty("rolloutTasks") final List<RolloutTask> rolloutTasks,
      @JsonProperty("taskIndex") final int taskIndex,
      @JsonProperty("deploymentGroup") final DeploymentGroup deploymentGroup) {
    this.rolloutTasks = checkNotNull(rolloutTasks, "rolloutTasks");
    this.taskIndex = taskIndex;
    this.deploymentGroup = deploymentGroup;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setRolloutTasks(rolloutTasks)
        .setTaskIndex(taskIndex)
        .setDeploymentGroup(deploymentGroup);
  }

  private DeploymentGroupTasks(final Builder builder) {
    this.rolloutTasks = checkNotNull(builder.rolloutTasks, "rolloutTasks");
    this.taskIndex = builder.taskIndex;
    this.deploymentGroup = checkNotNull(builder.deploymentGroup, "deploymentGroup");
  }

  public List<RolloutTask> getRolloutTasks() {
    return rolloutTasks;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public DeploymentGroup getDeploymentGroup() {
    return deploymentGroup;
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

    final DeploymentGroupTasks that = (DeploymentGroupTasks) o;

    if (taskIndex != that.taskIndex) {
      return false;
    }
    if (deploymentGroup != null ? !deploymentGroup.equals(that.deploymentGroup)
                                : that.deploymentGroup != null) {
      return false;
    }
    if (rolloutTasks != null ? !rolloutTasks.equals(that.rolloutTasks)
                             : that.rolloutTasks != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = rolloutTasks != null ? rolloutTasks.hashCode() : 0;
    result = 31 * result + taskIndex;
    result = 31 * result + (deploymentGroup != null ? deploymentGroup.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("rolloutTasks", rolloutTasks)
        .add("taskIndex", taskIndex)
        .add("deploymentGroup", deploymentGroup)
        .toString();
  }

  public static class Builder {
    private List<RolloutTask> rolloutTasks = Collections.emptyList();
    private int taskIndex;
    private DeploymentGroup deploymentGroup;

    public Builder setRolloutTasks(List<RolloutTask> rolloutTasks) {
      this.rolloutTasks = rolloutTasks;
      return this;
    }

    public Builder setTaskIndex(int taskIndex) {
      this.taskIndex = taskIndex;
      return this;
    }

    public Builder setDeploymentGroup(final DeploymentGroup deploymentGroup) {
      this.deploymentGroup = deploymentGroup;
      return this;
    }

    public DeploymentGroupTasks build() {
      return new DeploymentGroupTasks(this);
    }
  }
}
