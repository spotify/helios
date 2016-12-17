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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.RolloutTask;
import java.util.List;

public class RollingUndeployPlanner implements RolloutPlanner {

  private final DeploymentGroup deploymentGroup;

  private RollingUndeployPlanner(final DeploymentGroup deploymentGroup) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
  }

  public static RollingUndeployPlanner of(final DeploymentGroup deploymentGroup) {
    return new RollingUndeployPlanner(deploymentGroup);
  }

  @Override
  public List<RolloutTask> plan(final List<String> hosts) {
    // generate the rollout tasks
    final List<RolloutTask> rolloutTasks = Lists.newArrayList();
    final int parallelism = deploymentGroup.getRolloutOptions() != null
                            ? deploymentGroup.getRolloutOptions().getParallelism() : 1;

    Lists.partition(hosts, parallelism)
        .forEach(partition -> rolloutTasks.addAll(rolloutTasks(partition)));

    return ImmutableList.copyOf(rolloutTasks);
  }

  private List<RolloutTask> rolloutTasks(final List<String> hosts) {
    final ImmutableList.Builder<RolloutTask> tasks = ImmutableList.builder();
    hosts.forEach(host -> {
      tasks.add(RolloutTask.of(RolloutTask.Action.FORCE_UNDEPLOY_JOBS, host));
    });
    hosts.forEach(host -> {
      tasks.add(RolloutTask.of(RolloutTask.Action.AWAIT_UNDEPLOYED, host));
      tasks.add(RolloutTask.of(RolloutTask.Action.MARK_UNDEPLOYED, host));
    });
    return tasks.build();
  }
}
