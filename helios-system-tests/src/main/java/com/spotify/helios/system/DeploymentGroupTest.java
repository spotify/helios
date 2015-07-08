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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.MasterMain;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


public class DeploymentGroupTest extends SystemTestBase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TEST_GROUP = "my_group";
  private static final String TEST_LABEL = "foo=bar";

  private MasterMain master;

  @Before
  public void initialize() throws Exception {
    master = startDefaultMaster();

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
        cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux"))
        .get("status").asText());
    final String output = cli("inspect-deployment-group", "--json", TEST_GROUP);

    final DeploymentGroup dg = OBJECT_MAPPER.readValue(output, DeploymentGroup.class);

    assertEquals(TEST_GROUP, dg.getName());
    assertNull(dg.getJob());
    assertEquals(ImmutableMap.of("foo", "bar", "baz", "qux"), dg.getLabels());
  }

  @Test
  public void testCreateExistingSameDeploymentGroup() throws Exception {
    assertEquals("CREATED", Json.readTree(
        cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux"))
        .get("status").asText());
    assertEquals("NOT_MODIFIED", Json.readTree(
        cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux"))
        .get("status").asText());
  }

  @Test
  public void testCreateExistingConflictingDeploymentGroup() throws Exception {
    assertEquals("CREATED", Json.readTree(
        cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux"))
        .get("status").asText());
    assertEquals("CONFLICT", Json.readTree(
        cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar"))
        .get("status").asText());
  }

  @Test
  public void testRemoveDeploymentGroup() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    assertEquals("REMOVED", Json.readTree(
        cli("remove-deployment-group", "--json", TEST_GROUP))
        .get("status").asText());
  }

  @Test
  public void testRemoveNonExistingDeploymentGroup() throws Exception {
    assertEquals("DEPLOYMENT_GROUP_NOT_FOUND", Json.readTree(
        cli("remove-deployment-group", "--json", TEST_GROUP))
        .get("status").asText());
  }

  @Test
  public void testListDeploymentGroups() throws Exception {
    cli("create-deployment-group", "group2", "foo=bar");
    cli("create-deployment-group", "group1", "foo=bar");
    final String output = cli("list-deployment-groups", "--json");
    final List<String> deploymentGroups = OBJECT_MAPPER.readValue(
        output, new TypeReference<List<String>>() {
    });
    assertEquals(Arrays.asList("group1", "group2"), deploymentGroups);
  }

  @Test
  public void testRollingUpdate() throws Exception {
    final String firstHost = testHost();
    final String secondHost = testHost() + "2";

    // start two agents
    startDefaultAgent(firstHost, "--labels", TEST_LABEL);
    startDefaultAgent(secondHost, "--labels", TEST_LABEL);

    // create a deployment group  and job
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    // trigger a rolling update
    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", testJobNameAndVersion,
                                             TEST_GROUP),
                                         RollingUpdateResponse.class).getStatus());

    // ensure the job is running on both agents and the deployment group reaches DONE
    awaitTaskState(jobId, firstHost, TaskStatus.State.RUNNING);
    awaitTaskState(jobId, secondHost, TaskStatus.State.RUNNING);

    final Deployment deployment =  defaultClient().hostStatus(firstHost).get().getJobs().get(jobId);
    assertEquals(TEST_GROUP, deployment.getDeploymentGroupName());
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP,
                               DeploymentGroupStatus.State.DONE);

    // create a second job
    final String secondJobVersion = testJobVersion + "2";
    final String secondJobNameAndVersion = testJobNameAndVersion + "2";
    final JobId secondJobId = createJob(testJobName, secondJobVersion, BUSYBOX, IDLE_COMMAND);

    // trigger a rolling update to replace the first job with the second job
    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", secondJobNameAndVersion,
                                             TEST_GROUP),
                                         RollingUpdateResponse.class).getStatus());

    // ensure the second job rolled out fine
    awaitTaskState(secondJobId, firstHost, TaskStatus.State.RUNNING);
    awaitTaskState(secondJobId, secondHost, TaskStatus.State.RUNNING);
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP,
                               DeploymentGroupStatus.State.DONE);
  }

  @Test
  public void testAgentAddedAfterRollingUpdateIsDeployed() throws Exception {
    startDefaultAgent(testHost(), "--labels", "foo=bar");

    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar");
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update", testJobNameAndVersion, TEST_GROUP),
                                         RollingUpdateResponse.class).getStatus());

    awaitTaskState(jobId, testHost(), TaskStatus.State.RUNNING);

    // Rollout should be complete and on its second iteration at this point.
    // Start another agent and wait for it to have the job deployed to it.
    startDefaultAgent(testHost() + "2", "--labels", "foo=bar");

    awaitTaskState(jobId, testHost() + "2", TaskStatus.State.RUNNING);
  }

  @Test
  public void testRollingUpdateGroupNotFound() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertEquals(RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND,
                 OBJECT_MAPPER.readValue(cli("rolling-update", "--json", "my_job:2", "oops"),
                                         RollingUpdateResponse.class).getStatus());
  }

  @Test
  public void testAbortRollingUpdate() throws Exception {
    cli("create-deployment-group", "--json", TEST_GROUP, "foo=bar", "baz=qux");
    cli("create", "my_job:2", "my_image");
    assertThat(cli("abort-rolling-update", TEST_GROUP),
               containsString("Aborted rolling-update on deployment-group my_group"));
    final DeploymentGroupStatusResponse status = Json.read(
        cli("status-deployment-group", "--json", TEST_GROUP), DeploymentGroupStatusResponse.class);
    assertEquals(DeploymentGroupStatusResponse.Status.FAILED, status.getStatus());
    assertEquals("Aborted by user", status.getError());
  }


  @Test
  public void testAbortRollingUpdateGroupNotFound() throws Exception {
    assertThat(cli("abort-rolling-update", TEST_GROUP),
               containsString("Deployment-group my_group not found"));
  }

  @Test
  public void testRollingUpdateCoordination() throws Exception {
    // stop the default master
    master.stopAsync().awaitTerminated();

    // start a bunch of masters and agents
    final Map<String, MasterMain> masters = Maps.newHashMap();
    for (int i = 0; i < 3; i++) {
      final String name = TEST_MASTER + i;
      masters.put(name, startDefaultMaster("--name", name));
    }

    final Map<String, AgentMain> agents = Maps.newHashMap();
    for (int i = 0; i < 10; i++) {
      final String name = TEST_HOST + i;
      agents.put(name, startDefaultAgent(name, "--labels", TEST_LABEL));
    }

    // create a deployment group and start rolling out
    cli("create-deployment-group", "--json", TEST_GROUP, TEST_LABEL);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    assertEquals(RollingUpdateResponse.Status.OK,
                 OBJECT_MAPPER.readValue(cli("rolling-update",
                                             "--par", String.valueOf(agents.size()),
                                             testJobNameAndVersion,
                                             TEST_GROUP),
                                         RollingUpdateResponse.class).getStatus());

    // wait until the rollout is complete
    awaitDeploymentGroupStatus(defaultClient(), TEST_GROUP, DeploymentGroupStatus.State.DONE);

    // ensure that all masters were involved
    final Set<String> deployingMasters = Sets.newHashSet();
    final Map<String, HostStatus> hostStatuses = defaultClient().hostStatuses(
        Lists.newArrayList(agents.keySet())).get();
    for (final HostStatus status : hostStatuses.values()) {
      for (final Deployment deployment : status.getJobs().values()) {
        deployingMasters.add(deployment.getDeployerMaster());
      }
    }

    assertEquals(masters.size(), deployingMasters.size());
  }
}
