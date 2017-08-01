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

package com.spotify.helios.rollingupdate;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.DONE;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.ROLLING_OUT;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.create;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollingUpdateOpFactory {

  private static final Logger log = LoggerFactory.getLogger(RollingUpdateOpFactory.class);

  private final DeploymentGroupTasks tasks;
  private final DeploymentGroup deploymentGroup;
  private final DeploymentGroupEventFactory eventFactory;

  public RollingUpdateOpFactory(final DeploymentGroupTasks tasks,
                                final DeploymentGroupEventFactory eventFactory) {
    this.tasks = tasks;
    this.deploymentGroup = tasks.getDeploymentGroup();
    this.eventFactory = eventFactory;
  }

  public RollingUpdateOp start(final DeploymentGroup deploymentGroup,
                               final ZooKeeperClient client) throws KeeperException {
    client.ensurePath(Paths.statusDeploymentGroupTasks());

    final List<ZooKeeperOperation> ops = Lists.newArrayList();
    final List<Map<String, Object>> events = Lists.newArrayList();

    final List<RolloutTask> rolloutTasks = tasks.getRolloutTasks();
    events.add(eventFactory.rollingUpdateStarted(deploymentGroup));

    final Stat tasksStat = client.exists(
        Paths.statusDeploymentGroupTasks(deploymentGroup.getName()));
    if (tasksStat == null) {
      // Create the tasks path if it doesn't already exist. The following operations (delete or set)
      // assume the node already exists. If the tasks path is created/deleted before the transaction
      // is committed it will fail. This will on occasion generate a user-visible error but is
      // better than having inconsistent state.
      ops.add(create(Paths.statusDeploymentGroupTasks(deploymentGroup.getName())));
    }

    final DeploymentGroupStatus status;
    if (rolloutTasks.isEmpty()) {
      status = DeploymentGroupStatus.newBuilder()
          .setState(DONE)
          .build();
      ops.add(delete(Paths.statusDeploymentGroupTasks(deploymentGroup.getName())));
      events.add(eventFactory.rollingUpdateDone(deploymentGroup));
    } else {
      final DeploymentGroupTasks tasks = DeploymentGroupTasks.newBuilder()
          .setRolloutTasks(rolloutTasks)
          .setTaskIndex(0)
          .setDeploymentGroup(deploymentGroup)
          .build();
      status = DeploymentGroupStatus.newBuilder()
          .setState(ROLLING_OUT)
          .build();
      ops.add(set(Paths.statusDeploymentGroupTasks(deploymentGroup.getName()), tasks));
    }

    // NOTE: If the DG was removed this set() cause the transaction to fail, because removing
    // the DG removes this node. It's *important* that there's an operation that causes the
    // transaction to fail if the DG was removed or we'll end up with inconsistent state.
    ops.add(set(Paths.statusDeploymentGroup(deploymentGroup.getName()), status));

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
        events.add(eventFactory.rollingUpdateTaskSucceeded(deploymentGroup, task));
      }
    }

    return new RollingUpdateOp(ImmutableList.copyOf(ops), ImmutableList.copyOf(events));
  }

  /**
   * Don't advance to the next task -- yield and have the current task be executed again in the
   * next iteration.
   *
   * @return {@link RollingUpdateOp}
   */
  public RollingUpdateOp yield() {
    // Do nothing
    return new RollingUpdateOp(ImmutableList.<ZooKeeperOperation>of(),
        ImmutableList.<Map<String, Object>>of());
  }

  private boolean isIgnoreFailures() {
    return deploymentGroup.getRolloutOptions() != null
           && deploymentGroup.getRolloutOptions().getIgnoreFailures();
  }

  public RollingUpdateOp error(final String msg, final String host,
                               final RollingUpdateError errorCode,
                               final Map<String, Object> metadata) {
    final List<ZooKeeperOperation> operations = Lists.newArrayList();
    final String errMsg = isNullOrEmpty(host) ? msg : host + ": " + msg;

    if (isIgnoreFailures()) {
      log.info(
          "would have set state=FAILED for deploymentGroup={} but ignoreFailures is set to true "
          + "for this group/rollout. errorCode={} message={}",
          deploymentGroup.getName(), errorCode, errMsg);

      return nextTask(operations);
    }

    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setState(FAILED)
        .setError(errMsg)
        .build();

    // Delete tasks, set state to FAILED
    operations.add(delete(Paths.statusDeploymentGroupTasks(deploymentGroup.getName())));
    operations.add(set(Paths.statusDeploymentGroup(deploymentGroup.getName()), status));

    final RolloutTask task = tasks.getRolloutTasks().get(tasks.getTaskIndex());

    // Emit a FAILED event and a failed task event
    final List<Map<String, Object>> events = Lists.newArrayList();
    final Map<String, Object> taskEv = eventFactory.rollingUpdateTaskFailed(
        deploymentGroup, task, errMsg, errorCode, metadata);
    events.add(taskEv);
    events.add(eventFactory.rollingUpdateFailed(deploymentGroup, taskEv));

    return new RollingUpdateOp(ImmutableList.copyOf(operations),
        ImmutableList.copyOf(events));
  }

  public RollingUpdateOp error(final String msg, final String host,
                               final RollingUpdateError errorCode) {
    return error(msg, host, errorCode, Collections.<String, Object>emptyMap());
  }

  public RollingUpdateOp error(final Exception ex, final String host,
                               final RollingUpdateError errorCode) {
    final String message;
    if (errorCode == RollingUpdateError.PORT_CONFLICT) {
      message = ex.getMessage()
                + " (the conflicting job was deployed manually or by a different deployment group)";
    } else {
      message = ex.getMessage();
    }

    return error(message, host, errorCode);
  }
}
