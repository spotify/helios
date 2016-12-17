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

import static com.google.common.base.Preconditions.checkNotNull;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.RolloutTask;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class RollingUpdatePlanner implements RolloutPlanner {

  private final DeploymentGroup deploymentGroup;

  private RollingUpdatePlanner(final DeploymentGroup deploymentGroup) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
  }

  public static RollingUpdatePlanner of(final DeploymentGroup deploymentGroup) {
    return new RollingUpdatePlanner(deploymentGroup);
  }

  @Override
  public List<RolloutTask> plan(final List<String> hosts) {
    // generate the rollout tasks
    final List<RolloutTask> rolloutTasks = Lists.newArrayList();
    final int parallelism = deploymentGroup.getRolloutOptions() != null ?
                            deploymentGroup.getRolloutOptions().getParallelism() : 1;
    final boolean overlap = deploymentGroup.getRolloutOptions() != null &&
                            deploymentGroup.getRolloutOptions().getOverlap();

    for (final List<String> partition : Lists.partition(hosts, parallelism)) {
      rolloutTasks.addAll(overlap ? rolloutTasksWithOverlap(partition) : rolloutTasks(partition));
    }

    return ImmutableList.copyOf(rolloutTasks);
  }

  private List<RolloutTask> rolloutTasks(final List<String> hosts) {
    final ImmutableList.Builder<RolloutTask> result = ImmutableList.builder();
    for (final String host : hosts) {
      result.add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, host));
      result.add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, host));
    }
    for (final String host : hosts) {
      result.add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, host));
    }
    return result.build();
  }

  private List<RolloutTask> rolloutTasksWithOverlap(final List<String> hosts) {
    final ImmutableList.Builder<RolloutTask> result = ImmutableList.builder();
    for (final String host : hosts) {
      result.add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, host));
    }
    for (final String host : hosts) {
      result.add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, host));
    }
    for (final String host : hosts) {
      result.add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, host));
    }
    return result.build();
  }

}
