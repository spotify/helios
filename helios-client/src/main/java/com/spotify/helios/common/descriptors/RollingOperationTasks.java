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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RollingOperationTasks extends Descriptor {

  private final List<RolloutTask> rolloutTasks;
  private final int taskIndex;
  private final RollingOperation rollingOperation;

  private RollingOperationTasks(
      @JsonProperty("rolloutTasks") final List<RolloutTask> rolloutTasks,
      @JsonProperty("taskIndex") final int taskIndex,
      @JsonProperty("rollingOperation") final RollingOperation rollingOperation) {
    this.rolloutTasks = checkNotNull(rolloutTasks, "rolloutTasks");
    this.taskIndex = taskIndex;
    this.rollingOperation = rollingOperation;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setRolloutTasks(rolloutTasks)
        .setTaskIndex(taskIndex)
        .setRollingOperation(rollingOperation);
  }

  private RollingOperationTasks(final Builder builder) {
    this.rolloutTasks = checkNotNull(builder.rolloutTasks, "rolloutTasks");
    this.taskIndex = builder.taskIndex;
    this.rollingOperation = checkNotNull(builder.rollingOperation, "rollingOperation");
  }

  public List<RolloutTask> getRolloutTasks() {
    return rolloutTasks;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public RollingOperation getRollingOperation() {
    return rollingOperation;
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

    final RollingOperationTasks that = (RollingOperationTasks) o;

    if (taskIndex != that.taskIndex) {
      return false;
    }
    if (rollingOperation != null ? !rollingOperation.equals(that.rollingOperation)
                                : that.rollingOperation != null) {
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
    result = 31 * result + (rollingOperation != null ? rollingOperation.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RollingOperationTasks{" +
           "rolloutTasks=" + rolloutTasks +
           ", taskIndex=" + taskIndex +
           ", rollingOperation=" + rollingOperation +
           "} " + super.toString();
  }

  public static class Builder {
    private List<RolloutTask> rolloutTasks = Collections.emptyList();
    private int taskIndex;
    private RollingOperation rollingOperation;

    public Builder setRolloutTasks(List<RolloutTask> rolloutTasks) {
      this.rolloutTasks = rolloutTasks;
      return this;
    }

    public Builder setTaskIndex(int taskIndex) {
      this.taskIndex = taskIndex;
      return this;
    }

    public Builder setRollingOperation(final RollingOperation rollingOperation) {
      this.rollingOperation = rollingOperation;
      return this;
    }

    public RollingOperationTasks build() {
      return new RollingOperationTasks(this);
    }
  }
}
