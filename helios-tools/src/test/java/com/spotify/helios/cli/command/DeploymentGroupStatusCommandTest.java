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

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.RolloutTask;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentGroupStatusCommandTest {

  private static final Logger log = LoggerFactory.getLogger(DeploymentGroupStatusCommandTest.class);

  private static final String NAME = "foo-group";
  private static final String NON_EXISTENT_NAME = "bar-group";
  private static final Map<String, String> LABELS_MAP = ImmutableMap.of("foo", "bar", "baz", "qux");
  private static final String JOB_NAME = "foo-job";
  private static final String JOB_VERSION = "0.1.0";
  private static final JobId JOB_ID = new JobId(JOB_NAME, JOB_VERSION);
  private static final Job JOB = Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION).build();
  private static final List<RolloutTask> ROLLOUT_TASKS = ImmutableList.<RolloutTask>builder()
      .add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host1"))
      .add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host1"))
      .add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host1"))
      .add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host2"))
      .add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host2"))
      .add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host2"))
      .add(RolloutTask.of(RolloutTask.Action.UNDEPLOY_OLD_JOBS, "host3"))
      .add(RolloutTask.of(RolloutTask.Action.DEPLOY_NEW_JOB, "host3"))
      .add(RolloutTask.of(RolloutTask.Action.AWAIT_RUNNING, "host3"))
      .build();
  private static final String OTHER_DEPLOYER = "panda";
  private static final Map<String, Deployment> DEPLOYMENTS =
      ImmutableMap.<String, Deployment>builder()
          .put("host1", Deployment.of(JOB_ID, Goal.START, NAME))
          .put("host2", Deployment.of(JOB_ID, Goal.START, NAME))
          .put("host4", Deployment.of(JOB_ID, Goal.START, NAME))
          .put("host5", Deployment.of(JOB_ID, Goal.START, OTHER_DEPLOYER))
          .put("host6", Deployment.of(JOB_ID, Goal.START, OTHER_DEPLOYER)).build();
  private static final Map<String, TaskStatus> TASK_STATUSES = ImmutableMap.of(
      "host1", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      "host2", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
          .setState(TaskStatus.State.HEALTHCHECKING).build(),
      "host4", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      "host5", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build());

  private static final JobStatus JOB_STATUS = JobStatus.newBuilder().setJob(JOB)
      .setDeployments(DEPLOYMENTS).setTaskStatuses(TASK_STATUSES).build();

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private DeploymentGroupStatusCommand command;

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("status");

    command = new DeploymentGroupStatusCommand(subparser);

  }

  @Test
  public void testDeploymentGroupStatusCommand() throws Exception {
    final DeploymentGroup deploymentGroup =
        DeploymentGroup.newBuilder().setName(NAME).setLabels(LABELS_MAP).setJob(JOB_ID).build();

    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setRolloutTasks(ROLLOUT_TASKS)
        .setTaskIndex(3)
        .setState(DeploymentGroupStatus.State.ROLLING_OUT).build();

    when(client.deploymentGroupStatus(NAME)).thenReturn(Futures.immediateFuture(status));
    when(client.jobStatus(JOB_ID)).thenReturn(Futures.immediateFuture(JOB_STATUS));
    when(options.getString("name")).thenReturn(NAME);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    log.info(output);
    System.out.println(output);

    assertThat(output, containsString("Name: " + NAME));
    assertThat(output, containsString("Job: " + JOB_ID.toString()));
    assertThat(output, containsString("State: ACTIVE"));
    assertThat(output, containsString("HOST      DONE"));
    assertThat(output, containsString("host1.    X"));
    assertThat(output, containsString("host2."));
    assertThat(output, containsString("host3."));
    assertFalse(output.contains("host4"));
    assertFalse(output.contains("host5"));
    assertFalse(output.contains("host6"));
  }

  @Test
  public void testDeploymentGroupStatusCommandNotFound() throws Exception {
    final ListenableFuture<DeploymentGroupStatus> nullFuture = Futures.immediateFuture(null);
    when(client.deploymentGroupStatus(NON_EXISTENT_NAME)).thenReturn(nullFuture);

    when(options.getString("name")).thenReturn(NON_EXISTENT_NAME);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(1, ret);
    final String output = baos.toString();
    log.info(output);

    assertThat(output, containsString("Unknown deployment group: " + NON_EXISTENT_NAME));
  }

  @Test
  public void testDeploymentGroupStatusCommandNoMatchingDeployers() throws Exception {
    final Map<String, Deployment> deployments = ImmutableMap.of(
        "host2", Deployment.of(JOB_ID, Goal.START, OTHER_DEPLOYER),
        "host3", Deployment.of(JOB_ID, Goal.START, OTHER_DEPLOYER));

    final Map<String, TaskStatus> taskStatuses = ImmutableMap.of(
        "host2", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
            .setState(TaskStatus.State.RUNNING).build(),
        "host3", TaskStatus.newBuilder().setJob(JOB).setGoal(Goal.START)
            .setState(TaskStatus.State.RUNNING).build()
    );
    final JobStatus jobStatus = JobStatus.newBuilder().setJob(JOB)
        .setDeployments(deployments).setTaskStatuses(taskStatuses).build();

    final DeploymentGroup deploymentGroup =
        DeploymentGroup.newBuilder().setName(NAME).setLabels(LABELS_MAP).setJob(JOB_ID).build();
    final String errMsg =
        "job was already deployed, either manually or by a different deployment group";
    final DeploymentGroupStatus status = DeploymentGroupStatus.newBuilder()
        .setDeploymentGroup(deploymentGroup)
        .setRolloutTasks(ROLLOUT_TASKS)
        .setTaskIndex(3)
        .setError(errMsg)
        .setState(FAILED).build();

    when(client.deploymentGroupStatus(NAME)).thenReturn(Futures.immediateFuture(status));
    when(client.jobStatus(JOB_ID)).thenReturn(Futures.immediateFuture(jobStatus));

    when(options.getString("name")).thenReturn(NAME);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    log.info(output);

    assertThat(output, containsString("Name: " + NAME));
    assertThat(output, containsString("Job: " + JOB_ID.toString()));
    assertThat(output, containsString("State: " + FAILED));
    assertThat(output, containsString("Error: host2 - " + errMsg));
    assertThat(output, containsString("HOST      DONE"));
    assertThat(output, containsString("host1.    X"));
    assertThat(output, containsString("host2."));
    assertThat(output, containsString("host3."));
    assertFalse(output.contains("host4"));
    assertFalse(output.contains("host5"));
    assertFalse(output.contains("host6"));
  }
}