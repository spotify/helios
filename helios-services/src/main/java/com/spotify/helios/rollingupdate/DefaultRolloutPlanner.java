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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.RolloutTask;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultRolloutPlanner implements RolloutPlanner {

  private final RollingOperation rollingOp;

  private DefaultRolloutPlanner(final RollingOperation rollingOp) {
    this.rollingOp = checkNotNull(rollingOp, "rollingOp");
  }

  public static DefaultRolloutPlanner of(final RollingOperation rollingOp) {
    return new DefaultRolloutPlanner(rollingOp);
  }

  @Override
  public List<RolloutTask> plan(final Map<String, HostStatus> hostsAndStatuses) {
    // we only care about hosts that are UP
    final List<String> hosts = Lists.newArrayList();
    for (final Map.Entry<String, HostStatus> entry : hostsAndStatuses.entrySet()) {
      if (entry.getValue().getStatus().equals(HostStatus.Status.UP)) {
        hosts.add(entry.getKey());
      }
    }

    // generate the rollout tasks
    final List<RolloutTask> rolloutTasks = Lists.newArrayList();
    final int parallelism = rollingOp.getRolloutOptions() != null ?
                            rollingOp.getRolloutOptions().getParallelism() : 1;
    final boolean overlap = rollingOp.getRolloutOptions() != null &&
                            rollingOp.getRolloutOptions().getOverlap();

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
