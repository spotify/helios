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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.Delete;
import com.spotify.helios.servicescommon.coordination.SetData;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.spotify.helios.rollingupdate.DeploymentGroupEventFactory.RollingUpdateReason.HOSTS_CHANGED;
import static com.spotify.helios.rollingupdate.DeploymentGroupEventFactory.RollingUpdateReason.MANUAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RollingUpdateOpFactoryTest {

  private static final DeploymentGroup DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName("my_group")
      .setRolloutOptions(RolloutOptions.newBuilder().build())
      .build();

  private final DeploymentGroupEventFactory eventFactory = mock(DeploymentGroupEventFactory.class);

  @Test
  public void testStartManualNoHosts() {
    // Create a DeploymentGroupTasks object with no rolloutTasks (defaults to empty list).
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.start(DEPLOYMENT_GROUP, MANUAL, 0);

    // Two ZK operations should return:
    // * delete the tasks
    // * set the status to DONE
    assertEquals(
        ImmutableSet.of(
            new Delete("/status/deployment-group-tasks/my_group"),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.DONE)
                .setError(null)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(2, op.events().size());
    verify(eventFactory).rollingUpdateStarted(DEPLOYMENT_GROUP, MANUAL);
    verify(eventFactory).rollingUpdateDone(DEPLOYMENT_GROUP);
  }

  @Test
  public void testStartManualWithHosts() {
    // Create a DeploymentGroupTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.start(DEPLOYMENT_GROUP, MANUAL, 1);

    // Two ZK operations should return:
    // * set the task index to 0
    // * set the status to ROLLING_OUT
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-group-tasks/my_group", DeploymentGroupTasks.newBuilder()
                .setRolloutTasks(rolloutTasks)
                .setTaskIndex(0)
                .setDeploymentGroup(DEPLOYMENT_GROUP)
                .setNumTargets(1)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.ROLLING_OUT)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(DEPLOYMENT_GROUP, MANUAL);
  }

  @Test
  public void testStartHostsChanged() {
    // Create a DeploymentGroupTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.start(DEPLOYMENT_GROUP, HOSTS_CHANGED, 1);

    // Two ZK operations should return:
    // * set the task index to 0
    // * another to set the status to ROLLING_OUT
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-group-tasks/my_group", DeploymentGroupTasks.newBuilder()
                .setRolloutTasks(rolloutTasks)
                .setTaskIndex(0)
                .setDeploymentGroup(DEPLOYMENT_GROUP)
                .setNumTargets(1)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.ROLLING_OUT)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(DEPLOYMENT_GROUP, HOSTS_CHANGED);
  }

  @Test
  public void testNextTaskNoOps() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.nextTask();

    // A nexTask op with no ZK operations should result advancing the task index
    assertEquals(1, op.operations().size());
    assertEquals(new SetData("/status/deployment-group-tasks/my_group",
                             deploymentGroupTasks.toBuilder()
                                 .setTaskIndex(1)
                                 .build()
                                 .toJsonBytes()),
                 op.operations().get(0));

    // No events should be generated
    assertEquals(0, op.events().size());
  }

  @Test
  public void testNextTaskWithOps() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperOperation mockOp = mock(ZooKeeperOperation.class);
    final RollingUpdateOp op = opFactory.nextTask(Lists.newArrayList(mockOp));

    // A nextTask op with ZK operations should result in advancing the task index
    // and also contain the specified ZK operations
    assertEquals(
        ImmutableSet.of(
            mockOp,
            new SetData("/status/deployment-group-tasks/my_group",
                        deploymentGroupTasks.toBuilder()
                            .setTaskIndex(1)
                            .build()
                            .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // This is not a no-op -> an event should be emitted
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateTaskSucceeded(
        DEPLOYMENT_GROUP,
        deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex()));
  }

  @Test
  public void testTransitionToDone() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(2)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.nextTask();

    // When state -> DONE we expected
    //  * deployment group tasks are deleted
    //  * deployment group status is updated (to DONE)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.DONE)
                .setError(null)
                .build()
                .toJsonBytes()),
            new Delete("/status/deployment-group-tasks/my_group")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that an event is emitted
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateDone(DEPLOYMENT_GROUP);
  }

  @Test
  public void testTransitionToFailed() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op =
        opFactory.taskError("foo", "host1", RollingUpdateError.HOST_NOT_FOUND);

    final Map<String, Object> failEvent = Maps.newHashMap();
    when(eventFactory.rollingUpdateTaskFailed(
        any(DeploymentGroup.class), any(RolloutTask.class),
        anyString(), any(RollingUpdateError.class))).thenReturn(failEvent);

    // When state -> FAILED we expected
    //  * deployment group tasks are deleted
    //  * deployment group status is updated (to FAILED)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.FAILED)
                .setError("host1: foo")
                .build()
                .toJsonBytes()),
            new Delete("/status/deployment-group-tasks/my_group")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that a failed-task event and a rolling-update failed event are emitted
    assertEquals(2, op.events().size());

    verify(eventFactory).rollingUpdateTaskFailed(
        eq(DEPLOYMENT_GROUP),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString(),
        eq(RollingUpdateError.HOST_NOT_FOUND),
        eq(Collections.<String, Object>emptyMap()));

    verify(eventFactory).rollingUpdateFailed(
        eq(DEPLOYMENT_GROUP),
        eq(failEvent));
  }

  @Test
  public void testTaskError() {
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("my_group")
        .setRolloutOptions(RolloutOptions.newBuilder().setFailureThreshold(0.51f).build())
        .build();

    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host2"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host2"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host2")))
        .setDeploymentGroup(deploymentGroup)
        .setNumTargets(2)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op =
        opFactory.taskError("foo", "host1", RollingUpdateError.HOST_NOT_FOUND);

    final Map<String, Object> failEvent = Maps.newHashMap();
    when(eventFactory.rollingUpdateTaskFailed(
        any(DeploymentGroup.class), any(RolloutTask.class),
        anyString(), any(RollingUpdateError.class))).thenReturn(failEvent);

    // A nextTask op with ZK operations should result in advancing the task index,
    // adding a failed target, and contain the specified ZK operations.
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-group-tasks/my_group",
                        deploymentGroupTasks.toBuilder()
                            .setTaskIndex(1)
                            .addFailedTarget("host1")
                            .build()
                            .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // ...and that a rolling-update failed event is emitted
    assertEquals(1, op.events().size());

    verify(eventFactory).rollingUpdateTaskFailed(
        eq(deploymentGroup),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString(),
        eq(RollingUpdateError.HOST_NOT_FOUND));
  }

  @Test
  public void testTaskErrorTransitionToDone() {
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("my_group")
        .setRolloutOptions(RolloutOptions.newBuilder().setFailureThreshold((float) 100).build())
        .build();

    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(2)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(deploymentGroup)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op =
        opFactory.taskError("foo", "host1", RollingUpdateError.HOST_NOT_FOUND);

    final Map<String, Object> failEvent = Maps.newHashMap();
    when(eventFactory.rollingUpdateTaskFailed(
        any(DeploymentGroup.class), any(RolloutTask.class),
        anyString(), any(RollingUpdateError.class))).thenReturn(failEvent);

    // When state -> DONE we expect
    //  * deployment group tasks are deleted
    //  * deployment group status is updated (to DONE)
    //  * deployment group tasks is updated (failed target is added)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.DONE)
                .setError(null)
                .build()
                .toJsonBytes()),
            new Delete("/status/deployment-group-tasks/my_group")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that a failed-task event and a rolling-update failed event are emitted
    assertEquals(2, op.events().size());

    verify(eventFactory).rollingUpdateTaskFailed(
        eq(deploymentGroup),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString(),
        eq(RollingUpdateError.HOST_NOT_FOUND));

    verify(eventFactory).rollingUpdateDone(deploymentGroup);
  }

  @Test
  public void testTaskErrorTransitionToFailed() {
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("my_group")
        .setRolloutOptions(RolloutOptions.newBuilder().setFailureThreshold((float) 0).build())
        .build();

    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(2)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(deploymentGroup)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op =
        opFactory.taskError("foo", "host1", RollingUpdateError.HOST_NOT_FOUND);

    final Map<String, Object> failEvent = Maps.newHashMap();
    when(eventFactory.rollingUpdateTaskFailed(
        any(DeploymentGroup.class), any(RolloutTask.class),
        anyString(), any(RollingUpdateError.class))).thenReturn(failEvent);

    // When state -> DONE we expect
    //  * deployment group tasks are deleted
    //  * deployment group status is updated (to FAILED)
    assertEquals(
        ImmutableSet.of(
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.FAILED)
                .setError("host1: foo")
                .build()
                .toJsonBytes()),
            new Delete("/status/deployment-group-tasks/my_group")),
        ImmutableSet.copyOf(op.operations()));

    // ...and that a failed-task event and a rolling-update failed event are emitted
    assertEquals(2, op.events().size());

    verify(eventFactory).rollingUpdateTaskFailed(
        eq(deploymentGroup),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString(),
        eq(RollingUpdateError.HOST_NOT_FOUND),
        eq(Collections.<String, Object>emptyMap()));

    verify(eventFactory).rollingUpdateFailed(
        eq(deploymentGroup),
        eq(failEvent));
  }

  @Test
  public void testYield() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setNumTargets(1)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.yield();

    assertEquals(0, op.operations().size());
    assertEquals(0, op.events().size());
  }

  @Test
  public void testIsOverFailureThreshold() {
    assertTrue(RollingUpdateOpFactory.isOverFailureThreshold(1, 1, 0));
    assertFalse(RollingUpdateOpFactory.isOverFailureThreshold(1, 1, 1));
    assertTrue(RollingUpdateOpFactory.isOverFailureThreshold(1, 2, 0.5f));
    assertFalse(RollingUpdateOpFactory.isOverFailureThreshold(1, 2, 0.51f));
    assertTrue(RollingUpdateOpFactory.isOverFailureThreshold(1, 0, 0));
  }
}
