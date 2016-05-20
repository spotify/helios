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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.RolloutTask;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultRolloutPlannerTest {

  @Test
  public void testSerialRollout() {
    final RollingOperation rolling = RollingOperation.newBuilder()
        .setRolloutOptions(RolloutOptions.newBuilder()
                               .setParallelism(1)
                               .build())
        .build();
    final HostStatus statusUp = mock(HostStatus.class);
    when(statusUp.getStatus()).thenReturn(HostStatus.Status.UP);
    final Map<String, HostStatus> hostsAndStatuses = ImmutableMap.of(
        "agent1", statusUp,
        "agent2", statusUp,
        "agent3", statusUp,
        "agent4", statusUp
    );

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(rolling);

    final List<RolloutTask> tasks = rolloutPlanner.plan(hostsAndStatuses);

    final List<RolloutTask> expected = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent1"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent2"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent2"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent2"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent3"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent3"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent3"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent4"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent4"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent4"));

    assertEquals(expected, tasks);
  }

  @Test
  public void testParallelRollout() {
    final RollingOperation rolling = RollingOperation.newBuilder()
        .setRolloutOptions(RolloutOptions.newBuilder()
                               .setParallelism(2)
                               .build())
        .build();
    final HostStatus statusUp = mock(HostStatus.class);
    when(statusUp.getStatus()).thenReturn(HostStatus.Status.UP);
    final Map<String, HostStatus> hostsAndStatuses = ImmutableMap.of(
        "agent1", statusUp,
        "agent2", statusUp,
        "agent3", statusUp,
        "agent4", statusUp
    );

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(rolling);

    final List<RolloutTask> tasks = rolloutPlanner.plan(hostsAndStatuses);

    final List<RolloutTask> expected = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent1"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent2"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent2"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent2"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent3"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent3"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent4"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent4"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent3"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent4"));

    assertEquals(expected, tasks);
  }

  @Test
  public void testParallelRolloutWithRemainder() {
    final RollingOperation rolling = RollingOperation.newBuilder()
        .setRolloutOptions(RolloutOptions.newBuilder()
                               .setParallelism(3)
                               .build())
        .build();
    final HostStatus statusUp = mock(HostStatus.class);
    when(statusUp.getStatus()).thenReturn(HostStatus.Status.UP);
    final Map<String, HostStatus> hostsAndStatuses = ImmutableMap.of(
        "agent1", statusUp,
        "agent2", statusUp,
        "agent3", statusUp,
        "agent4", statusUp
    );

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(rolling);

    final List<RolloutTask> tasks = rolloutPlanner.plan(hostsAndStatuses);

    final List<RolloutTask> expected = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent1"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent2"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent2"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent3"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent3"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent2"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent3"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent4"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent4"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent4"));

    assertEquals(expected, tasks);
  }

  @Test
  public void testOverlapRollout() {
    final RollingOperation rolling = RollingOperation.newBuilder()
        .setRolloutOptions(RolloutOptions.newBuilder().setOverlap(true).build())
        .build();
    final HostStatus statusUp = mock(HostStatus.class);
    when(statusUp.getStatus()).thenReturn(HostStatus.Status.UP);
    final Map<String, HostStatus> hostsAndStatuses = ImmutableMap.of(
        "agent1", statusUp,
        "agent2", statusUp,
        "agent3", statusUp,
        "agent4", statusUp
    );

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(rolling);
    final List<RolloutTask> tasks = rolloutPlanner.plan(hostsAndStatuses);

    final List<RolloutTask> expected = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent1"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent2"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent2"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent2"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent3"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent3"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent3"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent4"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent4"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent4"));

    assertEquals(expected, tasks);
  }

  @Test
  public void testOverlapParallelRollout() {
    final RollingOperation rolling = RollingOperation.newBuilder()
        .setRolloutOptions(RolloutOptions.newBuilder()
                               .setOverlap(true)
                               .setParallelism(2)
                               .build())
        .build();
    final HostStatus statusUp = mock(HostStatus.class);
    when(statusUp.getStatus()).thenReturn(HostStatus.Status.UP);
    final Map<String, HostStatus> hostsAndStatuses = ImmutableMap.of(
        "agent1", statusUp,
        "agent2", statusUp,
        "agent3", statusUp,
        "agent4", statusUp
    );

    final RolloutPlanner rolloutPlanner = DefaultRolloutPlanner.of(rolling);
    final List<RolloutTask> tasks = rolloutPlanner.plan(hostsAndStatuses);

    final List<RolloutTask> expected = Lists.newArrayList(
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent1"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent2"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent1"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent2"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent1"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent2"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent3"),
        RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "agent4"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent3"),
        RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "agent4"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent3"),
        RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "agent4"));

    assertEquals(expected, tasks);
  }
}
