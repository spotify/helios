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

package com.spotify.helios.rollingupdate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
import com.spotify.helios.common.descriptors.RollingOperationTasks;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.STABLE;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.UNSTABLE;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.DONE;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.ROLLING_OUT;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;

public class RollingUpdateOpFactory {

  private final RollingOperationTasks tasks;
  private final RollingOperation rolling;
  private final RollingOperationEventFactory eventFactory;

  public RollingUpdateOpFactory(final RollingOperationTasks tasks,
                                final RollingOperationEventFactory eventFactory) {
    this.tasks = tasks;
    this.rolling = tasks.getRollingOperation();
    this.eventFactory = eventFactory;
  }

  public RollingUpdateOp start(final ZooKeeperClient client) throws KeeperException {
    final List<ZooKeeperOperation> ops = Lists.newArrayList();
    final List<Map<String, Object>> events = Lists.newArrayList();
    return start(client, ops, events);
  }

  public RollingUpdateOp start(final ZooKeeperClient client,
                               final List<ZooKeeperOperation> ops) throws KeeperException {
    final List<Map<String, Object>> events = Lists.newArrayList();
    return start(client, ops, events);
  }

  public RollingUpdateOp start(final ZooKeeperClient client,
                               final List<ZooKeeperOperation> ops,
                               final List<Map<String, Object>> events) throws KeeperException {
    client.ensurePath(Paths.statusRollingOpsTasks());

    final List<RolloutTask> rolloutTasks = tasks.getRolloutTasks();
    events.add(eventFactory.rollingUpdateStarted(rolling));

    final Stat tasksStat = client.exists(Paths.statusRollingOpsTasks(rolling.getId()));
    if (tasksStat == null) {
      // Create the tasks path if it doesn't already exist. The following operations (delete or set)
      // assume the node already exists. If the tasks path is created/deleted before the transaction
      // is committed it will fail. This will on occasion generate a user-visible error but is
      // better than having inconsistent state.
      ops.add(create(Paths.statusRollingOpsTasks(rolling.getId())));
    }

    final RollingOperationStatus status;
    if (rolloutTasks.isEmpty()) {
      // If there's no tasks the rolling operation is done, but we don't touch the deployment group
      // state because it's unchanged.
      status = RollingOperationStatus.newBuilder()
          .setState(DONE)
          .build();
      ops.add(delete(Paths.statusRollingOpsTasks(rolling.getId())));
      events.add(eventFactory.rollingUpdateDone(rolling));
    } else {
      final DeploymentGroupStatus dgStatus = DeploymentGroupStatus.newBuilder()
          .setState(UNSTABLE)
          .build();
      ops.add(set(Paths.statusDeploymentGroup(rolling.getDeploymentGroupName()), dgStatus));

      status = RollingOperationStatus.newBuilder()
          .setState(ROLLING_OUT)
          .build();
      ops.add(set(Paths.statusRollingOpsTasks(rolling.getId()), tasks));
    }

    ops.add(set(Paths.statusRollingOp(rolling.getId()), status));

    return new RollingUpdateOp(ImmutableList.copyOf(ops), ImmutableList.copyOf(events));
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
      final RollingOperationStatus status = RollingOperationStatus.newBuilder()
          .setState(DONE)
          .build();

      final DeploymentGroupStatus dgStatus = DeploymentGroupStatus.newBuilder()
          .setState(STABLE)
          .build();

      // We are done -> delete tasks & update status
      ops.add(delete(Paths.statusRollingOpsTasks(rolling.getId())));
      ops.add(set(Paths.statusRollingOp(rolling.getId()), status));
      ops.add(set(Paths.statusDeploymentGroup(rolling.getDeploymentGroupName()), dgStatus));

      // Emit an event signalling that we're DONE!
      events.add(eventFactory.rollingUpdateDone(rolling));
    } else {
      ops.add(set(Paths.statusRollingOpsTasks(rolling.getId()),
                  tasks.toBuilder()
                      .setTaskIndex(tasks.getTaskIndex() + 1)
                      .build()));

      // Only emit an event if the task resulted in taking in action. If there are no ZK operations
      // the task was effectively a no-op.
      if (!operations.isEmpty()) {
        events.add(eventFactory.rollingUpdateTaskSucceeded(rolling, task));
      }
    }

    return new RollingUpdateOp(ImmutableList.copyOf(ops), ImmutableList.copyOf(events));
  }

  /**
   * Don't advance to the next task -- yield and have the current task be executed again in the
   * next iteration.
   * @return {@link RollingUpdateOp}
   */
  public RollingUpdateOp yield() {
    // Do nothing
    return new RollingUpdateOp(ImmutableList.<ZooKeeperOperation>of(),
                               ImmutableList.<Map<String, Object>>of());
  }

  public RollingUpdateOp error(final String msg, final String host,
                               final RollingOperationError errorCode,
                               final Map<String, Object> metadata) {
    final List<ZooKeeperOperation> operations = Lists.newArrayList();
    final String errMsg = isNullOrEmpty(host) ? msg : host + ": " + msg;

    final RollingOperationStatus rollingStatus = RollingOperationStatus.newBuilder()
        .setState(FAILED)
        .setError(errMsg)
        .build();

    final DeploymentGroupStatus deploymentGroupStatus = DeploymentGroupStatus.newBuilder()
        .setState(UNSTABLE)
        .setError("Rolling operation " + rolling.getId() + ": " + errMsg)
        .build();

    // Delete tasks, set rolling operation state to FAILED and deployment group state to UNSTABLE.
    operations.add(delete(Paths.statusRollingOpsTasks(rolling.getId())));
    operations.add(set(Paths.statusRollingOp(rolling.getId()), rollingStatus));
    operations.add(
        set(Paths.statusDeploymentGroup(rolling.getDeploymentGroupName()), deploymentGroupStatus));

    final RolloutTask task = tasks.getRolloutTasks().get(tasks.getTaskIndex());

    // Emit a FAILED event and a failed task event
    final List<Map<String, Object>> events = Lists.newArrayList();
    final Map<String, Object> taskEv = eventFactory.rollingUpdateTaskFailed(
        rolling, task, errMsg, errorCode, metadata);
    events.add(taskEv);
    events.add(eventFactory.rollingUpdateFailed(rolling, taskEv));

    return new RollingUpdateOp(ImmutableList.copyOf(operations),
                               ImmutableList.copyOf(events));
  }

  public RollingUpdateOp error(final String msg, final String host,
                               final RollingOperationError errorCode) {
    return error(msg, host, errorCode, Collections.<String, Object>emptyMap());
  }

  public RollingUpdateOp error(final Exception e, final String host,
                               final RollingOperationError errorCode) {
    return error(e.getMessage(), host, errorCode);
  }
}
