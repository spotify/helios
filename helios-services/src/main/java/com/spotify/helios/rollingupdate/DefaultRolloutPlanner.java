/*
 * Copyright (c) 2014 Spotify AB.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.RolloutTask;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultRolloutPlanner implements RolloutPlanner {

  private final DeploymentGroup deploymentGroup;
  private final List<String> hosts;

  private DefaultRolloutPlanner(final DeploymentGroup deploymentGroup,
                                final List<String> hosts) {
    this.deploymentGroup = checkNotNull(deploymentGroup, "deploymentGroup");
    this.hosts = checkNotNull(hosts, "hosts");
  }

  public static DefaultRolloutPlanner of(final DeploymentGroup deploymentGroup,
                                         final List<String> hosts) {
    return new DefaultRolloutPlanner(deploymentGroup, hosts);
  }

  @Override
  public List<RolloutTask> plan() {
    final List<RolloutTask> rolloutTasks = Lists.newArrayList();

    final int parallelism = deploymentGroup.getRolloutOptions() != null ?
                            deploymentGroup.getRolloutOptions().getParallelism() : 1;

    for (final List<String> partition : Lists.partition(hosts, parallelism)) {
      for (final String host : partition) {
        rolloutTasks.add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, host));
      }
      for (final String host : partition) {
        rolloutTasks.add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, host));
      }
      for (final String host : partition) {
        rolloutTasks.add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, host));
      }
    }

    return ImmutableList.copyOf(rolloutTasks);
  }
}
