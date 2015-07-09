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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentGroupStatusCommandTest {

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
  public void testDeploymentGroupStatus() throws Exception {
    final String name = "foo-group";
    final String jobName = "foo-job";
    final String jobVersion = "0.1.0";
    final JobId jobId = new JobId(jobName, jobVersion);

    final List<DeploymentGroupStatusResponse.HostStatus> hostStatuses = Lists.newArrayList();
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host1", jobId, TaskStatus.State.RUNNING));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host2", jobId, TaskStatus.State.PULLING_IMAGE));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host3", null, null));

    final DeploymentGroupStatusResponse status = new DeploymentGroupStatusResponse(
        name, DeploymentGroupStatusResponse.Status.ROLLING_OUT, jobId, null, hostStatuses, null);

    when(client.deploymentGroupStatus(name)).thenReturn(Futures.immediateFuture(status));
    when(options.getString("name")).thenReturn(name);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString().replaceAll("\\s+", "");

    final String expected =
        format("Name: %s" +
               "Job Id: %s" +
               "Status: ROLLING_OUT" +
               "HOST UP-TO-DATE JOB STATE" +
               "host1. X %s RUNNING" +
               "host2. X %s PULLING_IMAGE" +
               "host3. - -",
               name, jobId, jobId, jobId).replace(" ", "");

    assertEquals(expected, output);
  }

  @Test
  public void testDeploymentGroupStatusWithError() throws Exception {
    final String name = "foo-group";
    final String jobName = "foo-job";
    final String jobVersion = "0.1.0";
    final JobId jobId = new JobId(jobName, jobVersion);

    final List<DeploymentGroupStatusResponse.HostStatus> hostStatuses = Lists.newArrayList();
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host1", jobId, TaskStatus.State.RUNNING));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host2", jobId, TaskStatus.State.PULLING_IMAGE));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host3", null, null));

    final DeploymentGroupStatusResponse status = new DeploymentGroupStatusResponse(
        name, DeploymentGroupStatusResponse.Status.ROLLING_OUT, jobId, "Oops!", hostStatuses, null);

    when(client.deploymentGroupStatus(name)).thenReturn(Futures.immediateFuture(status));
    when(options.getString("name")).thenReturn(name);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString().replaceAll("\\s+", "");

    final String expected =
        format("Name: %s" +
               "Job Id: %s" +
               "Status: ROLLING_OUT" +
               "Error: Oops!" +
               "HOST UP-TO-DATE JOB STATE" +
               "host1. X %s RUNNING" +
               "host2. X %s PULLING_IMAGE" +
               "host3. - -",
               name, jobId, jobId, jobId).replace(" ", "");

    assertEquals(expected, output);
  }

  @Test
  public void testDeploymentGroupNotFound() throws Exception {
    final ListenableFuture<DeploymentGroupStatusResponse> nullFuture = Futures.immediateFuture(null);
    when(client.deploymentGroupStatus(anyString())).thenReturn(nullFuture);

    final String name = "non-existent-group";
    when(options.getString("name")).thenReturn(name);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(1, ret);
    final String output = baos.toString();

    assertThat(output, containsString(format("Unknown deployment group: %s", name)));
  }

  @Test
  public void testDeploymentGroupStatusJson() throws Exception {
    final String name = "foo-group";
    final String jobName = "foo-job";
    final String jobVersion = "0.1.0";
    final JobId jobId = new JobId(jobName, jobVersion);

    final List<DeploymentGroupStatusResponse.HostStatus> hostStatuses = Lists.newArrayList();
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host1", jobId, TaskStatus.State.RUNNING));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host2", jobId, TaskStatus.State.RUNNING));
    hostStatuses.add(new DeploymentGroupStatusResponse.HostStatus(
        "host3", jobId, TaskStatus.State.PULLING_IMAGE));

    final DeploymentGroupStatusResponse status = new DeploymentGroupStatusResponse(
        name, DeploymentGroupStatusResponse.Status.ROLLING_OUT, jobId, null, hostStatuses, null);

    when(client.deploymentGroupStatus(name)).thenReturn(Futures.immediateFuture(status));
    when(options.getString("name")).thenReturn(name);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(0, ret);
    final DeploymentGroupStatusResponse output = Json.read(
        baos.toString(), DeploymentGroupStatusResponse.class);

    assertEquals(status, output);
  }

  @Test
  public void testDeploymentGroupNotFoundJson() throws Exception {
    final ListenableFuture<DeploymentGroupStatusResponse> nullFuture = Futures.immediateFuture(null);
    when(client.deploymentGroupStatus(anyString())).thenReturn(nullFuture);

    final String name = "non-existent-group";
    when(options.getString("name")).thenReturn(name);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(1, ret);
    final Map<String, Object> output = Json.read(
        baos.toString(), new TypeReference<Map<String, Object>>() {});


    assertEquals("DEPLOYMENT_GROUP_NOT_FOUND", output.get("status"));
  }
}