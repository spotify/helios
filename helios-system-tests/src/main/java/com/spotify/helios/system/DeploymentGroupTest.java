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

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.RollingUpdateResponse;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


public class DeploymentGroupTest extends SystemTestBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Before
  public void initialize() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });
  }

  @Test
  public void testGetNonExistingDeploymentGroup() throws Exception {
    final String output = cli("inspect-deployment-group", "--json", "not_there");
    assertThat(output, containsString("Unknown deployment group: not_there"));
  }

  @Test
  public void testCreateDeploymentGroup() throws Exception {
    assertEquals("CREATED", Json.readTree(
        cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux"))
        .get("status").asText());
    final String output = cli("inspect-deployment-group", "--json", "my_group");

    final DeploymentGroup dg = OBJECT_MAPPER.readValue(output, DeploymentGroup.class);

    assertEquals("my_group", dg.getName());
    assertNull(dg.getJob());
    assertEquals(ImmutableMap.of("foo", "bar", "baz", "qux"), dg.getLabels());
  }

  @Test
  public void testCreateExistingSameDeploymentGroup() throws Exception {
    assertEquals("CREATED", Json.readTree(
        cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux"))
        .get("status").asText());
    assertEquals("NOT_MODIFIED", Json.readTree(
        cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux"))
        .get("status").asText());
  }

  @Test
  public void testCreateExistingConflictingDeploymentGroup() throws Exception {
    assertEquals("CREATED", Json.readTree(
        cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux"))
        .get("status").asText());
    assertEquals("CONFLICT", Json.readTree(
        cli("create-deployment-group", "--json", "my_group", "foo=bar"))
        .get("status").asText());
  }

  @Test
  public void testRemoveDeploymentGroup() throws Exception {
    cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux");
    assertEquals("REMOVED", Json.readTree(
        cli("remove-deployment-group", "--json", "my_group"))
        .get("status").asText());
  }

  @Test
  public void testRemoveNonExistingDeploymentGroup() throws Exception {
    assertEquals("DEPLOYMENT_GROUP_NOT_FOUND", Json.readTree(
        cli("remove-deployment-group", "--json", "my_group"))
        .get("status").asText());
  }

  @Test
  public void testListDeploymentGroups() throws Exception {
    cli("create-deployment-group", "group2", "foo=bar");
    cli("create-deployment-group", "group1", "foo=bar");
    final String output = cli("list-deployment-groups", "--json");
    final List<String> deploymentGroups = OBJECT_MAPPER.readValue(
        output, new TypeReference<List<String>>(){});
    assertEquals(Arrays.asList("group1", "group2"), deploymentGroups);
  }

  @Test
  public void testRollingUpdate() throws Exception {
    startDefaultAgent(testHost(), "--labels", "foo=bar");
    startDefaultAgent(testHost() + "2", "--labels", "foo=bar");

    cli("create-deployment-group", "--json", "my_group", "foo=bar");
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", testJobNameAndVersion, "my_group"),
                                         RollingUpdateResponse.class).getStatus());

    awaitTaskState(jobId, testHost() + "2", TaskStatus.State.RUNNING);
    Thread.sleep(10);

    final JobId secondJob = createJob(testJobName, testJobVersion + "2", BUSYBOX, IDLE_COMMAND);

    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", testJobNameAndVersion + "2",
                                             "my_group"),
                                         RollingUpdateResponse.class).getStatus());

    awaitTaskState(secondJob, testHost() + "2", TaskStatus.State.RUNNING);
  }

  @Test
  public void testAgentAddedAfterRollingUpdateIsDeployed() throws Exception {
    startDefaultAgent(testHost(), "--labels", "foo=bar");

    cli("create-deployment-group", "--json", "my_group", "foo=bar");
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", testJobNameAndVersion, "my_group"),
                                         RollingUpdateResponse.class).getStatus());

    awaitTaskState(jobId, testHost(), TaskStatus.State.RUNNING);

    // Rollout should be complete and on its second iteration at this point.
    // Start another agent and wait for it to have the job deployed to it.
    startDefaultAgent(testHost() + "2", "--labels", "foo=bar");

    awaitTaskState(jobId, testHost() + "2", TaskStatus.State.RUNNING);
  }

  @Test
  public void testRollingUpdateGroupNotFound() throws Exception {
    cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertEquals(RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND,
                 OBJECT_MAPPER.readValue(cli("rolling-update", "--json", "my_job:2", "oops"),
                                         RollingUpdateResponse.class).getStatus());
  }

  @Test
  public void testAbortRollingUpdate() throws Exception {
    cli("create-deployment-group", "--json", "my_group", "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertThat(cli("abort-rolling-update", "my_group"),
               containsString("Aborted rolling-update on deployment-group my_group"));
    // TODO(staffan): Verify status is FAILED and error set appropriately
  }

  @Test
  public void testAbortRollingUpdateGroupNotFound() throws Exception {
    assertThat(cli("abort-rolling-update", "my_group"),
               containsString("Deployment-group my_group not found"));
  }
}
