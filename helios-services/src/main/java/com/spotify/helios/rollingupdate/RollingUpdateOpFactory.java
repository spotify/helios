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

package com.spotify.helios.rollingupdate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.DONE;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;

public class RollingUpdateOpFactory {

  private final DeploymentGroupTasks tasks;
  private final DeploymentGroup deploymentGroup;
  private final DeploymentGroupEventFactory eventFactory;

  public RollingUpdateOpFactory(final DeploymentGroupTasks tasks,
                                final DeploymentGroupEventFactory eventFactory) {
    this.tasks = tasks;
    this.deploymentGroup = tasks.getDeploymentGroup();
    this.eventFactory = eventFactory;
  }

  public RollingUpdateOp nextTask() {
    return nextTask(Collections.<ZooKeeperOperation>emptyList());
  }

  public RollingUpdateOp nextTask(final List<ZooKeeperOperation> operations) {
    final List<ZooKeeperOperation> ops = Lists.newArrayList(operations);
    final List<Map<String, Object>> events = Lists.newArrayList();

    final RolloutTask task = tasks.getRolloutTasks().get(tasks.getTaskIndex());

    // Update the task index, delete tasks if done
    if (tasks.getTaskIndex() + 1 == tasks.getRolloutTasks().size()) {
      final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
          .setState(DONE)
          .build();

      // We are done -> delete tasks & update status
      ops.add(delete(Paths.statusDeploymentGroupTasks(deploymentGroup.getName())));
      ops.add(set(Paths.statusDeploymentGroup(deploymentGroup.getName()),
                  status));

      // Emit an event signalling that we're DONE!
      events.add(eventFactory.rollingUpdateDone(deploymentGroup));
    } else {
      ops.add(
          set(Paths.statusDeploymentGroupTasks(deploymentGroup.getName()), tasks.toBuilder()
              .setTaskIndex(tasks.getTaskIndex() + 1)
              .build()));

      // Only emit an event if the task resulted in taking in action. If there are no ZK operations
      // the task was effectively a no-op.
      if (!operations.isEmpty()) {
        events.add(eventFactory.rollingUpdateTaskOk(deploymentGroup, task));
      }
    }

    return new RollingUpdateOp(ImmutableList.copyOf(ops), ImmutableList.copyOf(events));
  }

  /**
   * Don't advance to the next task -- yield and have the current task be executed again in the
   * next iteration.
   */
  public RollingUpdateOp yield() {
    // Do nothing
    return new RollingUpdateOp(ImmutableList.<ZooKeeperOperation>of(),
                               ImmutableList.<Map<String, Object>>of());
  }

  public RollingUpdateOp error(final String msg, final String host) {
    final List<ZooKeeperOperation> operations = Lists.newArrayList();
    final String errMsg = isNullOrEmpty(host) ? msg : host + ": " + msg;

    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setState(FAILED)
        .setError(errMsg)
        .build();

    // Delete tasks, set state to FAILED
    operations.add(delete(Paths.statusDeploymentGroupTasks(deploymentGroup.getName())));
    operations.add(set(Paths.statusDeploymentGroup(deploymentGroup.getName()), status));

    final RolloutTask task = tasks.getRolloutTasks().get(tasks.getTaskIndex());

    // Emit a FAILED event
    final List<Map<String, Object>> events = Lists.newArrayList();
    events.add(eventFactory.rollingUpdateFailed(deploymentGroup, task, errMsg));

    return new RollingUpdateOp(ImmutableList.copyOf(operations),
                               ImmutableList.copyOf(events));
  }

  public RollingUpdateOp error(final Exception e, final String host) {
    return error(e.getMessage(), host);
  }
}
