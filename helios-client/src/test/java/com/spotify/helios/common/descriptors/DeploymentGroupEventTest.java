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

package com.spotify.helios.common.descriptors;

import com.spotify.helios.common.Json;

import org.junit.Test;

import java.io.IOException;

import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.ROLLING_OUT;
import static org.junit.Assert.assertEquals;

public class DeploymentGroupEventTest {

  private static final String JOB_NAME = "foo";
  private static final String JOB_VERSION = "17";
  private static final String JOB_HASH = "deadbeef";
  private static final JobId JOB_ID = JobId.newBuilder()
      .setName(JOB_NAME)
      .setVersion(JOB_VERSION)
      .setHash(JOB_HASH)
      .build();

  private static final String TARGET = "host1";
  private static final String GROUP_NAME = "foo-group";
  private static final DeploymentGroup DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName(GROUP_NAME)
      .setJobId(JOB_ID)
      .build();

  private static final long TIMESTAMP = 8675309L;

  @Test
  public void testJsonParsing() throws IOException {
    final String json = "{\"action\":\"AWAIT_RUNNING\",\"deploymentGroupStatus\":"
                                + "{\"deploymentGroup\":{\"hostSelectors\":[],\"name\":\""
                                + GROUP_NAME + "\",\"jobId\":\"" + JOB_ID + "\"},"
                                + "\"error\":null,\"rolloutTasks\":[],\"state\":\"ROLLING_OUT\","
                                + "\"successfulIterations\":0,\"taskIndex\":0},"
                                + "\"rolloutTaskStatus\":\"OK\",\"target\":\"" + TARGET + "\","
                                + "\"timestamp\":" + TIMESTAMP + "}";
    final DeploymentGroupEvent event = Json.read(json, DeploymentGroupEvent.class);

    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .setState(ROLLING_OUT)
        .build();

    final DeploymentGroupEvent expectedEvent = DeploymentGroupEvent.newBuilder()
        .setAction(RolloutTask.Action.AWAIT_RUNNING)
        .setTarget(TARGET)
        .setRolloutTaskStatus(RolloutTask.Status.OK)
        .setDeploymentGroupStatus(status)
        .setTimestamp(TIMESTAMP)
        .build();

    assertEquals(expectedEvent, event);
  }

  @Test
  public void testJsonSerialization() throws IOException {
    final String expectedJson = "{\"action\":\"AWAIT_RUNNING\",\"deploymentGroupStatus\":"
                                + "{\"deploymentGroup\":{\"hostSelectors\":[],\"name\":\""
                                + GROUP_NAME + "\",\"jobId\":\"" + JOB_ID + "\"},"
                                + "\"error\":null,\"rolloutTasks\":[],\"state\":\"ROLLING_OUT\","
                                + "\"successfulIterations\":0,\"taskIndex\":0},"
                                + "\"rolloutTaskStatus\":\"OK\",\"target\":\"" + TARGET + "\","
                                + "\"timestamp\":" + TIMESTAMP + "}";

    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setState(ROLLING_OUT)
        .setDeploymentGroup(DEPLOYMENT_GROUP)
        .build();

    final DeploymentGroupEvent event = DeploymentGroupEvent.newBuilder()
        .setAction(RolloutTask.Action.AWAIT_RUNNING)
        .setTarget(TARGET)
        .setRolloutTaskStatus(RolloutTask.Status.OK)
        .setDeploymentGroupStatus(status)
        .setTimestamp(TIMESTAMP)
        .build();

    final String json = Json.asStringUnchecked(event);
    assertEquals(expectedJson, json);
  }
}
