/*
 * Copyright (c) 2014 Spotify AB.
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.RolloutTask;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RollingUndeployPlanner implements RolloutPlanner {

  private final DeploymentGroup deploymentGroup;

  private RollingUndeployPlanner(final DeploymentGroup deploymentGroup) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
  }

  public static RollingUndeployPlanner of(final DeploymentGroup deploymentGroup) {
    return new RollingUndeployPlanner(deploymentGroup);
  }

  @Override
  public List<RolloutTask> plan(final Map<String, HostStatus> hostsAndStatuses) {
    // we only care about hosts that are UP
    final List<String> hosts = hostsAndStatuses.entrySet()
        .stream()
        .filter(entry -> entry.getValue().getStatus().equals(HostStatus.Status.UP))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());

    // generate the rollout tasks
    final List<RolloutTask> rolloutTasks = Lists.newArrayList();
    final int parallelism = deploymentGroup.getRolloutOptions() != null
                            ? deploymentGroup.getRolloutOptions().getParallelism() : 1;

    Lists.partition(hosts, parallelism)
        .forEach(partition -> rolloutTasks.addAll(rolloutTasks(partition)));

    return ImmutableList.copyOf(rolloutTasks);
  }

  private List<RolloutTask> rolloutTasks(final List<String> hosts) {
    return hosts.stream()
        .map(host -> RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, host))
        .collect(Collectors.toList());
  }
}
