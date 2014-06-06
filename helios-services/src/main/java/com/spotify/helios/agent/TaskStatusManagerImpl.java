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

package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.ThrottleState;

import java.util.Map;

public class TaskStatusManagerImpl implements TaskStatusManager {
  private final AgentModel model;
  private final Job job;

  private boolean isFlapping;
  private State status;
  private TaskStatus taskStatus;
  private ThrottleState throttle;

  public TaskStatusManagerImpl(AgentModel model, Job job) {
    this.model = model;
    this.job = job;
  }

  @Override
  public void updateFlappingState(boolean isFlapping) {
    if (isFlapping == this.isFlapping) {
      return;
    }

    this.isFlapping = isFlapping;
    if (this.isFlapping && throttle == ThrottleState.NO) {
      throttle = ThrottleState.FLAPPING;
    } else if (throttle == ThrottleState.FLAPPING && isFlapping == false) {
      throttle = ThrottleState.NO;
    }

    updateModelStatus(taskStatus.asBuilder());
  }

  @Override
  public boolean isFlapping() {
    return isFlapping;
  }

  @Override
  public void setStatus(Goal goal, State status, ThrottleState throttle, String containerId,
                        Map<String, PortMapping> ports, Map<String, String> env) {

    this.throttle = throttle;
    this.status = status;

    TaskStatus.Builder builder = TaskStatus.newBuilder()
        .setJob(job)
        .setGoal(goal)
        .setState(status)
        .setContainerId(containerId)
        .setPorts(ports)
        .setEnv(env);

    updateModelStatus(builder);
  }

  private void updateModelStatus(TaskStatus.Builder builder) {
    builder.setThrottled(throttle);
    model.setTaskStatus(job.getId(), builder.build());
    taskStatus = builder.build();
  }

  @Override
  public State getStatus() {
    return status;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private AgentModel model;
    private Job job;

    private Builder() {}

    public Builder setModel(AgentModel model) {
      this.model = model;
      return this;
    }

    public Builder setJob(Job job) {
      this.job = job;
      return this;
    }

    public TaskStatusManagerImpl build() {
      return new TaskStatusManagerImpl(model, job);
    }
  }
}
