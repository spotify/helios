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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.SetData;
import com.spotify.helios.servicescommon.coordination.Delete;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RollingUpdateOpFactoryTest {

  private static final DeploymentGroup DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName("my_group")
      .setRolloutOptions(RolloutOptions.newBuilder().build())
      .build();
  private static final Map<String, Object> DONE_EVENT =
      ImmutableMap.<String, Object>of("foo", "done_event");
  private static final Map<String, Object> FAILED_EVENT =
      ImmutableMap.<String, Object>of("foo", "fail_event");
  private static final Map<String, Object> TASK_EVENT =
      ImmutableMap.<String, Object>of("foo", "task_event");

  private final DeploymentGroupEventFactory eventFactory = mock(DeploymentGroupEventFactory.class);

  @Test
  public void testNextTaskNoOps() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(DEPLOYMENT_GROUP)
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
                                 .toJsonBytes()), op.operations().get(0));

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
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperOperation mockOp = mock(ZooKeeperOperation.class);
    final RollingUpdateOp op = opFactory.nextTask(Lists.newArrayList(mockOp));

    // A nexTask op with ZK operations should result in advancing the task index
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
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.error("foo", "host1");

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

    // ...and that an event is emitted
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateFailed(
        eq(DEPLOYMENT_GROUP),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString());
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
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.yield();

    assertEquals(0, op.operations().size());
    assertEquals(0, op.events().size());
  }
}
