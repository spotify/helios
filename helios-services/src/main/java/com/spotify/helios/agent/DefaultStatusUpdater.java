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

import com.google.common.base.Supplier;

import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;

import java.util.concurrent.atomic.AtomicReference;

public class DefaultStatusUpdater implements StatusUpdater {

  private final TaskConfig taskConfig;
  private final AtomicReference<ThrottleState> throttle;
  private final AtomicReference<Goal> goal;
  private final TaskStatusManager statusManager;
  private final Supplier<String> containerIdSupplier;

  public DefaultStatusUpdater(final AtomicReference<Goal> goal,
                              final AtomicReference<ThrottleState> throttle,
                              final TaskConfig taskConfig,
                              final TaskStatusManager statusManager,
                              final Supplier<String> containerIdSupplier) {
    this.goal = goal;
    this.throttle = throttle;
    this.taskConfig = taskConfig;
    this.statusManager = statusManager;
    this.containerIdSupplier = containerIdSupplier;
  }

  /**
   * Persist job status with port mapping.
   */
  @Override
  public void setStatus(final TaskStatus.State status) throws InterruptedException {
    setStatus(status, containerIdSupplier.get());
  }

  @Override
  public void setStatus(final TaskStatus.State status, final String containerId)
      throws InterruptedException {
    statusManager.setStatus(goal.get(), status, throttle.get(),
                            containerId, taskConfig.ports(),
                            taskConfig.containerEnv());
  }

}
