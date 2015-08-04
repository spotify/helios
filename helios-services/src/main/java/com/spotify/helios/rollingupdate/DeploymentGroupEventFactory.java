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

import com.google.common.collect.Maps;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.RolloutTask;

import java.util.Map;

public class DeploymentGroupEventFactory {

  public enum RollingUpdateReason {
    MANUAL,
    HOSTS_CHANGED
  }

  public Map<String, Object> rollingUpdateTaskOk(final DeploymentGroup deploymentGroup,
                                                 final RolloutTask task) {
    final Map<String, Object> ev = Maps.newHashMap();
    ev.put("timestamp", System.currentTimeMillis());
    ev.put("deploymentGroupName", deploymentGroup.getName());
    ev.put("action", task.getAction());
    ev.put("target", task.getTarget());
    ev.put("result", RolloutTask.Status.OK);
    return ev;
  }

  public Map<String, Object> rollingUpdateStarted(final DeploymentGroup deploymentGroup,
                                                  final RollingUpdateReason reason) {
    final Map<String, Object> ev = Maps.newHashMap();
    ev.put("timestamp", System.currentTimeMillis());
    ev.put("deploymentGroupName", deploymentGroup.getName());
    ev.put("reason", reason);
    return ev;
  }

  public Map<String, Object> rollingUpdateDone(final DeploymentGroup deploymentGroup) {
    final Map<String, Object> ev = Maps.newHashMap();
    ev.put("timestamp", System.currentTimeMillis());
    ev.put("deploymentGroupName", deploymentGroup.getName());
    ev.put("state", DeploymentGroupStatus.State.DONE);
    return ev;
  }

  public Map<String, Object> rollingUpdateFailed(final DeploymentGroup deploymentGroup,
                                                 final RolloutTask task,
                                                 final String error) {
    final Map<String, Object> ev = Maps.newHashMap();
    ev.put("timestamp", System.currentTimeMillis());
    ev.put("deploymentGroupName", deploymentGroup.getName());
    ev.put("state", DeploymentGroupStatus.State.FAILED);
    ev.put("action", task.getAction());
    ev.put("target", task.getTarget());
    ev.put("result", RolloutTask.Status.FAILED);
    ev.put("error", error);
    return ev;
  }
}
