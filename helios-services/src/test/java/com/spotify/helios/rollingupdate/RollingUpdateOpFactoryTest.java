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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
import com.spotify.helios.common.descriptors.RollingOperationTasks;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.CreateEmpty;
import com.spotify.helios.servicescommon.coordination.Delete;
import com.spotify.helios.servicescommon.coordination.SetData;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.STABLE;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.UNSTABLE;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.DONE;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.RollingOperationStatus.State.ROLLING_OUT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RollingUpdateOpFactoryTest {

  private static final RollingOperation MANUAL_ROLLING_OPERATION =
      RollingOperation.newBuilder()
          .setId("uuid")
          .setDeploymentGroupName("my_group")
          .setRolloutOptions(RolloutOptions.newBuilder().build())
          .setReason(RollingOperation.Reason.MANUAL)
          .build();

  private static final RollingOperation HOSTS_CHANGED_ROLLING_OPERATION =
      RollingOperation.newBuilder()
          .setId("uuid")
          .setDeploymentGroupName("my_group")
          .setRolloutOptions(RolloutOptions.newBuilder().build())
          .setReason(RollingOperation.Reason.MANUAL)
          .build();

  private final RollingOperationEventFactory eventFactory =
      mock(RollingOperationEventFactory.class);

  @Test
  public void testStartManualNoHosts() throws Exception {
    // Create a RollingOperationTasks object with no rolloutTasks (defaults to empty list).
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(client);

    // Three ZK operations should return:
    // * create tasks node
    // * delete the tasks
    // * set the rolling operation status to DONE
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/rolling-operation-tasks/uuid"),
            new Delete("/status/rolling-operation-tasks/uuid"),
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(DONE)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(2, op.events().size());
    verify(eventFactory).rollingUpdateStarted(MANUAL_ROLLING_OPERATION);
    verify(eventFactory).rollingUpdateDone(MANUAL_ROLLING_OPERATION);
  }

  @Test
  public void testStartManualNoHostsTasksAlreadyExist() throws Exception {
    // Create a RollingOperationTasks object with no rolloutTasks (defaults to empty list).
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(mock(Stat.class));
    final RollingUpdateOp op = opFactory.start(client);

    // Two ZK operations should return:
    // * delete the tasks
    // * set the rolling operation status to DONE
    assertEquals(
        ImmutableSet.of(
            new Delete("/status/rolling-operation-tasks/uuid"),
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(DONE)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(2, op.events().size());
    verify(eventFactory).rollingUpdateStarted(MANUAL_ROLLING_OPERATION);
    verify(eventFactory).rollingUpdateDone(MANUAL_ROLLING_OPERATION);
  }

  @Test
  public void testStartManualWithHosts() throws Exception {
    // Create a RollingOperationTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(client);

    // Four ZK operations should return:
    // * create tasks node
    // * set the task index to 0
    // * set the rolling operation status to ROLLING_OUT
    // * set the deployment group status to UNSTABLE
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/rolling-operation-tasks/uuid"),
            new SetData(
                "/status/rolling-operation-tasks/uuid",
                RollingOperationTasks.newBuilder()
                    .setRolloutTasks(rolloutTasks)
                    .setTaskIndex(0)
                    .setRollingOperation(MANUAL_ROLLING_OPERATION)
                    .build()
                    .toJsonBytes()),
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(ROLLING_OUT)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(UNSTABLE)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(MANUAL_ROLLING_OPERATION);
  }

  @Test
  public void testStartHostsChanged() throws Exception {
    // Create a RollingOperationTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setRollingOperation(HOSTS_CHANGED_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(client);

    // Four ZK operations should return:
    // * create tasks node
    // * set the task index to 0
    // * set the rolling operation status to ROLLING_OUT
    // * set the deployment group status to UNSTABLE
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/rolling-operation-tasks/uuid"),
            new SetData(
                "/status/rolling-operation-tasks/uuid",
                RollingOperationTasks.newBuilder()
                    .setRolloutTasks(rolloutTasks)
                    .setTaskIndex(0)
                    .setRollingOperation(HOSTS_CHANGED_ROLLING_OPERATION)
                    .build()
                    .toJsonBytes()),
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(ROLLING_OUT)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(UNSTABLE)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(HOSTS_CHANGED_ROLLING_OPERATION);
  }

  @Test
  public void testNextTaskNoOps() {
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final RollingUpdateOp op = opFactory.nextTask();

    // A nexTask op with no ZK operations should result advancing the task index
    assertEquals(1, op.operations().size());
    assertEquals(new SetData("/status/rolling-operation-tasks/uuid",
                             rollingOperationTasks.toBuilder()
                                 .setTaskIndex(1)
                                 .build()
                                 .toJsonBytes()), op.operations().get(0));

    // No events should be generated
    assertEquals(0, op.events().size());
  }

  @Test
  public void testNextTaskWithOps() {
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final ZooKeeperOperation mockOp = mock(ZooKeeperOperation.class);
    final RollingUpdateOp op = opFactory.nextTask(Lists.newArrayList(mockOp));

    // A nexTask op with ZK operations should result in advancing the task index
    // and also contain the specified ZK operations
    assertEquals(
        ImmutableSet.of(
            mockOp,
            new SetData("/status/rolling-operation-tasks/uuid",
                        rollingOperationTasks.toBuilder()
                            .setTaskIndex(1)
                            .build()
                            .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // This is not a no-op -> an event should be emitted
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateTaskSucceeded(
        MANUAL_ROLLING_OPERATION,
        rollingOperationTasks.getRolloutTasks().get(rollingOperationTasks.getTaskIndex()));
  }

  @Test
  public void testTransitionToDone() {
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(2)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final RollingUpdateOp op = opFactory.nextTask();

    // When state -> DONE we expected
    //  * rolling operation tasks are deleted
    //  * rolling operation status is updated (to DONE)
    //  * deployment group status is updated (to STABLE)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(DONE)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(STABLE)
                .setError(null)
                .build()
                .toJsonBytes()),
            new Delete("/status/rolling-operation-tasks/uuid")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that an event is emitted
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateDone(MANUAL_ROLLING_OPERATION);
  }

  @Test
  public void testTransitionToFailed() {
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final RollingUpdateOp op = opFactory.error(
        "foo", "host1", RollingOperationError.HOST_NOT_FOUND);

    final Map<String, Object> failEvent = Maps.newHashMap();
    when(eventFactory.rollingUpdateTaskFailed(
        any(RollingOperation.class), any(RolloutTask.class),
        anyString(), any(RollingOperationError.class))).thenReturn(failEvent);

    // When state -> FAILED we expected
    //  * rolling operation tasks are deleted
    //  * rolling operation status is updated (to FAILED)
    //  * deployment group status is updated (to UNSTABLE)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(UNSTABLE)
                .setError("Rolling operation uuid: host1: foo")
                .build()
                .toJsonBytes()),
            new SetData("/status/rolling-operations/uuid", RollingOperationStatus.newBuilder()
                .setState(FAILED)
                .setError("host1: foo")
                .build()
                .toJsonBytes()),
            new Delete("/status/rolling-operation-tasks/uuid")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that a failed-task event and a rolling-update failed event are emitted
    assertEquals(2, op.events().size());

    verify(eventFactory).rollingUpdateTaskFailed(
        eq(MANUAL_ROLLING_OPERATION),
        eq(rollingOperationTasks.getRolloutTasks().get(rollingOperationTasks.getTaskIndex())),
        anyString(),
        eq(RollingOperationError.HOST_NOT_FOUND),
        eq(Collections.<String, Object>emptyMap()));

    verify(eventFactory).rollingUpdateFailed(
        eq(MANUAL_ROLLING_OPERATION),
        eq(failEvent));
  }

  @Test
  public void testYield() {
    final RollingOperationTasks rollingOperationTasks = RollingOperationTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setRollingOperation(MANUAL_ROLLING_OPERATION)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        rollingOperationTasks, eventFactory);
    final RollingUpdateOp op = opFactory.yield();

    assertEquals(0, op.operations().size());
    assertEquals(0, op.events().size());
  }
}
