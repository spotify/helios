/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;

/**
 * Plain boring status updater for tasks to report their status to the {@link AgentModel}.
 */
public class DefaultStatusUpdater implements StatusUpdater {

  private final TaskStatus.Builder builder;

  private Goal goal;
  private String containerId;
  private ThrottleState throttleState = ThrottleState.NO;
  private AgentModel model;
  private TaskStatus.State state;
  private String containerError;

  public DefaultStatusUpdater(final AgentModel model,
                              final TaskStatus.Builder builder) {
    this.model = model;
    this.builder = builder;
  }

  @Override
  public void setThrottleState(final ThrottleState throttleState) {
    this.throttleState = throttleState;
  }

  @Override
  public void setContainerId(final String containerId) {
    this.containerId = containerId;
  }

  @Override
  public void setGoal(final Goal goal) {
    this.goal = goal;
  }

  @Override
  public void setContainerError(final String containerError) {
    this.containerError = containerError;
  }

  @Override
  public void update() throws InterruptedException {
    final TaskStatus status = builder
        .setGoal(goal)
        .setState(state)
        .setContainerId(containerId)
        .setThrottled(throttleState)
        .setContainerError(containerError)
        .build();
    model.setTaskStatus(status.getJob().getId(), status);
  }

  @Override
  public void setState(final TaskStatus.State state) {
    this.state = state;
  }
}
