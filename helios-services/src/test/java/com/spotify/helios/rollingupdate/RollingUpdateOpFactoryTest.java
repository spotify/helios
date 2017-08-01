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

import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.HOSTS_CHANGED;
import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.MANUAL;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.DeploymentGroupTasks;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.servicescommon.coordination.CreateEmpty;
import com.spotify.helios.servicescommon.coordination.Delete;
import com.spotify.helios.servicescommon.coordination.SetData;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class RollingUpdateOpFactoryTest {

  private static final DeploymentGroup MANUAL_DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName("my_group")
      .setRolloutOptions(RolloutOptions.newBuilder().build())
      .setRollingUpdateReason(MANUAL)
      .build();

  private static final DeploymentGroup HOSTS_CHANGED_DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName("my_group")
      .setRolloutOptions(RolloutOptions.newBuilder().build())
      .setRollingUpdateReason(HOSTS_CHANGED)
      .build();

  private final DeploymentGroupEventFactory eventFactory = mock(DeploymentGroupEventFactory.class);

  @Test
  public void testStartManualNoHosts() throws Exception {
    // Create a DeploymentGroupTasks object with no rolloutTasks (defaults to empty list).
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(MANUAL_DEPLOYMENT_GROUP, client);

    // Three ZK operations should return:
    // * create tasks node
    // * delete the tasks
    // * set the status to DONE
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/deployment-group-tasks/my_group"),
            new Delete("/status/deployment-group-tasks/my_group"),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.DONE)
                .setError(null)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(2, op.events().size());
    verify(eventFactory).rollingUpdateStarted(MANUAL_DEPLOYMENT_GROUP);
    verify(eventFactory).rollingUpdateDone(MANUAL_DEPLOYMENT_GROUP);
  }

  @Test
  public void testStartManualNoHostsTasksAlreadyExist() throws Exception {
    // Create a DeploymentGroupTasks object with no rolloutTasks (defaults to empty list).
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(mock(Stat.class));
    final RollingUpdateOp op = opFactory.start(MANUAL_DEPLOYMENT_GROUP, client);

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
    verify(eventFactory).rollingUpdateStarted(MANUAL_DEPLOYMENT_GROUP);
    verify(eventFactory).rollingUpdateDone(MANUAL_DEPLOYMENT_GROUP);
  }

  @Test
  public void testStartManualWithHosts() throws Exception {
    // Create a DeploymentGroupTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(MANUAL_DEPLOYMENT_GROUP, client);

    // Three ZK operations should return:
    // * create tasks node
    // * set the task index to 0
    // * set the status to ROLLING_OUT
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/deployment-group-tasks/my_group"),
            new SetData("/status/deployment-group-tasks/my_group", DeploymentGroupTasks.newBuilder()
                .setRolloutTasks(rolloutTasks)
                .setTaskIndex(0)
                .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.ROLLING_OUT)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(MANUAL_DEPLOYMENT_GROUP);
  }

  @Test
  public void testStartHostsChanged() throws Exception {
    // Create a DeploymentGroupTasks object with some rolloutTasks.
    final ArrayList<RolloutTask> rolloutTasks = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
    );
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(rolloutTasks)
        .setDeploymentGroup(HOSTS_CHANGED_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final ZooKeeperClient client = mock(ZooKeeperClient.class);
    when(client.exists(anyString())).thenReturn(null);
    final RollingUpdateOp op = opFactory.start(HOSTS_CHANGED_DEPLOYMENT_GROUP, client);

    // Three ZK operations should return:
    // * create tasks node
    // * set the task index to 0
    // * another to set the status to ROLLING_OUT
    assertEquals(
        ImmutableSet.of(
            new CreateEmpty("/status/deployment-group-tasks/my_group"),
            new SetData("/status/deployment-group-tasks/my_group", DeploymentGroupTasks.newBuilder()
                .setRolloutTasks(rolloutTasks)
                .setTaskIndex(0)
                .setDeploymentGroup(HOSTS_CHANGED_DEPLOYMENT_GROUP)
                .build()
                .toJsonBytes()),
            new SetData("/status/deployment-groups/my_group", DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.ROLLING_OUT)
                .build()
                .toJsonBytes())),
        ImmutableSet.copyOf(op.operations()));

    // Two events should return: rollingUpdateStarted and rollingUpdateDone
    assertEquals(1, op.events().size());
    verify(eventFactory).rollingUpdateStarted(HOSTS_CHANGED_DEPLOYMENT_GROUP);
  }

  @Test
  public void testNextTaskNoOps() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.nextTask();

    // A nextTask op with no ZK operations should result advancing the task index
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
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
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
        MANUAL_DEPLOYMENT_GROUP,
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
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
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
    verify(eventFactory).rollingUpdateDone(MANUAL_DEPLOYMENT_GROUP);
  }

  @Test
  public void testTransitionToFailed() {
    final DeploymentGroupTasks deploymentGroupTasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(0)
        .setRolloutTasks(Lists.newArrayList(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1")))
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.error("foo", "host1", RollingUpdateError.HOST_NOT_FOUND);

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
        eq(MANUAL_DEPLOYMENT_GROUP),
        eq(deploymentGroupTasks.getRolloutTasks().get(deploymentGroupTasks.getTaskIndex())),
        anyString(),
        eq(RollingUpdateError.HOST_NOT_FOUND),
        eq(Collections.<String, Object>emptyMap()));

    verify(eventFactory).rollingUpdateFailed(
        eq(MANUAL_DEPLOYMENT_GROUP),
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
        .setDeploymentGroup(MANUAL_DEPLOYMENT_GROUP)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(
        deploymentGroupTasks, eventFactory);
    final RollingUpdateOp op = opFactory.yield();

    assertEquals(0, op.operations().size());
    assertEquals(0, op.events().size());
  }

  @Test
  public void testErrorWhenIgnoreFailuresIsTrue() {
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("ignore_failure_group")
        .setRolloutOptions(RolloutOptions.newBuilder()
            .setIgnoreFailures(true)
            .build()
        )
        .setRollingUpdateReason(MANUAL)
        .build();

    // the current task is the AWAIT_RUNNING one
    final DeploymentGroupTasks tasks = DeploymentGroupTasks.newBuilder()
        .setTaskIndex(2)
        .setRolloutTasks(ImmutableList.of(
            RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"),
            RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"),
            RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1")
        ))
        .setDeploymentGroup(deploymentGroup)
        .build();

    final RollingUpdateOpFactory opFactory = new RollingUpdateOpFactory(tasks, eventFactory);

    final RollingUpdateOp nextOp = opFactory.error("something went wrong", "host1",
        RollingUpdateError.TIMED_OUT_WAITING_FOR_JOB_TO_REACH_RUNNING);

    assertThat(nextOp.operations(), containsInAnyOrder(
        new SetData("/status/deployment-groups/ignore_failure_group",
            DeploymentGroupStatus.newBuilder()
                .setState(DeploymentGroupStatus.State.DONE)
                .setError(null)
                .build()
                .toJsonBytes()
        ),
        new Delete("/status/deployment-group-tasks/ignore_failure_group")
    ));
  }
}
