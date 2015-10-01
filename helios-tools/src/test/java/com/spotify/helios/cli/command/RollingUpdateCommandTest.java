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

package com.spotify.helios.cli.command;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse.RolloutState;
import com.spotify.helios.common.protocol.RollingUpdateResponse;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.RolloutOptions.DEFAULT_FAILURE_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RollingUpdateCommandTest {

  private static final String GROUP_NAME = "my_group";
  private static final JobId JOB_ID = new JobId("foo", "2", "1212121212121212121");
  private static final JobId OLD_JOB_ID = new JobId("foo", "1", "3232323232323232323");
  private static final JobId NEW_JOB_ID = new JobId("foo", "3", "4242424242424242424");
  private static final int PARALLELISM = 1;
  private static final long TIMEOUT = 300;
  private static final String TOKEN = "my_token";

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);
  private final TimeUtil timeUtil = new TimeUtil();
  private final RollingUpdateCommand command = new RollingUpdateCommand(
      ArgumentParsers.newArgumentParser("test").addSubparsers().addParser("rolling-update"),
      timeUtil, timeUtil);

  @Before
  public void before() {
    // Default CLI argument stubs
    when(options.getString("deployment-group-name")).thenReturn(GROUP_NAME);
    when(options.getInt("parallelism")).thenReturn(PARALLELISM);
    when(options.getLong("timeout")).thenReturn(TIMEOUT);
    when(options.getLong("rollout_timeout")).thenReturn(10L);
    when(options.getBoolean("async")).thenReturn(false);
    when(options.getBoolean("migrate")).thenReturn(false);
    when(options.getBoolean("overlap")).thenReturn(false);
    when(options.getFloat("failure_threshold")).thenReturn(DEFAULT_FAILURE_THRESHOLD);
  }

  private static DeploymentGroupStatusResponse.HostStatus makeHostStatus(
      final String host, final JobId jobId, final State state) {
    return makeHostStatus(host, jobId, state, null);
  }

  private static DeploymentGroupStatusResponse.HostStatus makeHostStatus(
      final String host, final JobId jobId, final State state, final RolloutState rolloutState) {
    return makeHostStatus(host, jobId, state, rolloutState, null);
  }

  private static DeploymentGroupStatusResponse.HostStatus
  makeHostStatus(final String host, final JobId jobId, final State state,
                 final RolloutState rolloutState, final String errMsg) {
    return new DeploymentGroupStatusResponse.HostStatus(host, jobId, state, rolloutState, errMsg);
  }

  private static DeploymentGroupStatusResponse statusResponse(
      final DeploymentGroupStatusResponse.Status status, final String error,
      DeploymentGroupStatusResponse.HostStatus... args) {
    return statusResponse(status, JOB_ID, error, args);
  }

  private static DeploymentGroupStatusResponse statusResponse(
      final DeploymentGroupStatusResponse.Status status, final JobId jobId, final String error,
      DeploymentGroupStatusResponse.HostStatus... args) {
    return new DeploymentGroupStatusResponse(
        new DeploymentGroup(GROUP_NAME, Collections.<HostSelector>emptyList(), jobId,
                            RolloutOptions.newBuilder().build()),
        status, error, Arrays.asList(args), null);
  }

  @Test
  public void testRollingUpdate() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", OLD_JOB_ID, State.RUNNING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", JOB_ID, State.PULLING_IMAGE, RolloutState.PENDING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host3", JOB_ID, State.CREATING, RolloutState.PENDING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host3", JOB_ID, State.RUNNING, RolloutState.DONE))
    ));

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final String expected = String.format(
        "Rolling update started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=%.2f)\n" +
        "\n" +
        "host1 -> DONE (1/3)\n" +
        "host2 -> DONE (2/3)\n" +
        "host3 -> DONE (3/3)\n" +
        "\n" +
        "Done.\n" +
        "Duration: 4.00 s\n",
        DEFAULT_FAILURE_THRESHOLD
    );

    assertEquals(expected, output.replaceAll("\\p{Blank}+|(?:\\p{Blank})$", " "));
  }

  @Test
  public void testRollingUpdateAsync() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(options.getBoolean("async")).thenReturn(true);

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final String expected =
        "Rolling update (async) started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=0.00)\n";

    assertEquals(expected, output);
  }

  @Test
  public void testRollingUpdateFailsIfJobIdChangedDuringRollout() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", OLD_JOB_ID, State.RUNNING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", JOB_ID, State.PULLING_IMAGE, RolloutState.PENDING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, NEW_JOB_ID, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", JOB_ID, State.STARTING, RolloutState.PENDING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING))
    ));

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final String expected =
        "Rolling update started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=0.00)\n" +
        "\n" +
        "host1 -> DONE (1/3)\n" +
        "\n" +
        "Failed: Deployment-group job id changed during rolling-update\n" +
        "Duration: 2.00 s\n";

    assertEquals(expected, output.replaceAll("\\p{Blank}+|(?:\\p{Blank})$", " "));
  }

  @Test
  public void testRollingUpdateFailsOnRolloutTimeout() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.PULLING_IMAGE),
                       makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final String expected =
        "Rolling update started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=0.00)\n" +
        "\n" +
        "\n" +
        "Timed out! (rolling-update still in progress)\n" +
        "Duration: 601.00 s\n";

    assertEquals(expected, output.replaceAll("\\p{Blank}+|(?:\\p{Blank})$", " "));
  }

  @Test
  public void testRollingUpdateFailed() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.PULLING_IMAGE, RolloutState.PENDING),
                       makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.FAILED, "foobar",
                       makeHostStatus("host1", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final String expected =
        "Rolling update started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=0.00)\n" +
        "\n" +
        "host1 -> DONE (1/2)\n" +
        "\n" +
        "Failed: foobar\n" +
        "Duration: 1.00 s\n";

    assertEquals(expected, output.replaceAll("\\p{Blank}+|(?:\\p{Blank})$", " "));
  }

  // ----------------------------

  @Test
  public void testRollingUpdateJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING),
                       makeHostStatus("host2", JOB_ID, State.RUNNING),
                       makeHostStatus("host3", JOB_ID, State.RUNNING))
    ));

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "DONE");
    expected.put("duration", 0.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateAsyncJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(options.getBoolean("async")).thenReturn(true);

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "OK");
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateFailsIfJobIdChangedDuringRolloutJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", OLD_JOB_ID, State.RUNNING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, NEW_JOB_ID, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING),
                       makeHostStatus("host2", JOB_ID, State.STARTING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING))
    ));

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "FAILED");
    expected.put("error", "Deployment-group job id changed during rolling-update");
    expected.put("duration", 1.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateFailsOnRolloutTimeoutJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.PULLING_IMAGE),
                       makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "TIMEOUT");
    expected.put("duration", 601.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateFailedJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.PULLING_IMAGE),
                       makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.FAILED, "foobar",
                       makeHostStatus("host1", JOB_ID, State.RUNNING),
                       makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(1, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "FAILED");
    expected.put("error", "foobar");
    expected.put("duration", 1.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateMigrate() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING))
    ));
    when(options.getBoolean("migrate")).thenReturn(true);

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    // Verify that rollingUpdate() was called with migrate=true
    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, true, false, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "DONE");
    expected.put("duration", 0.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", false);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }
  
  @Test
  public void testRollingUpdateOverlapJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING))
    ));
    when(options.getBoolean("overlap")).thenReturn(true);

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    // Verify that rollingUpdate() was called with migrate=true
    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, true, null, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    final Map<String, Object> expected = Maps.newHashMap();
    expected.put("status", "DONE");
    expected.put("duration", 0.00);
    expected.put("parallelism", PARALLELISM);
    expected.put("timeout", TIMEOUT);
    expected.put("overlap", true);
    expected.put("token", null);
    expected.put("failureThreshold", DEFAULT_FAILURE_THRESHOLD);
    assertJsonOutputEquals(output, expected);
  }

  @Test
  public void testRollingUpdateWithToken() throws Exception {
    when(options.getString("token")).thenReturn(TOKEN);
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.RUNNING))
    ));
    when(options.getBoolean("overlap")).thenReturn(true);

    final int ret = command.runWithJobId(options, client, out, true, JOB_ID, null);
    final String output = baos.toString();

    // Verify that rollingUpdate() was called with migrate=true
    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, true, TOKEN, DEFAULT_FAILURE_THRESHOLD));
    assertEquals(0, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "DONE")
        .put("duration", 0.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", true)
        .put("token", TOKEN)
        .put("failureThreshold", DEFAULT_FAILURE_THRESHOLD)
        .build());
  }

  @Test
  public void testRollingUpdateFailureThreshold() throws Exception {
    when(options.getFloat("failure_threshold")).thenReturn(0.5f);

    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", null, null),
                       makeHostStatus("host2", OLD_JOB_ID, State.RUNNING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.FAILED, RolloutState.FAILED, "health check failed"),
                       makeHostStatus("host2", JOB_ID, State.PULLING_IMAGE, RolloutState.PENDING),
                       makeHostStatus("host3", OLD_JOB_ID, State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
                       makeHostStatus("host1", JOB_ID, State.FAILED, RolloutState.FAILED, "health check failed"),
                       makeHostStatus("host2", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host3", JOB_ID, State.CREATING, RolloutState.PENDING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
                       makeHostStatus("host1", JOB_ID, State.FAILED, RolloutState.FAILED, "health check failed"),
                       makeHostStatus("host2", JOB_ID, State.RUNNING, RolloutState.DONE),
                       makeHostStatus("host3", JOB_ID, State.RUNNING, RolloutState.DONE))
    ));

    final int ret = command.runWithJobId(options, client, out, false, JOB_ID, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(
        GROUP_NAME, JOB_ID,
        new RolloutOptions(TIMEOUT, PARALLELISM, false, false, null, 0.5f));
    assertEquals(0, ret);

    final String expected = (
        "Rolling update started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=null, failure threshold=0.50)\n" +
        "\n" +
        "host1 -> FAILED health check failed (1/3)\n" +
        "host2 -> DONE (2/3)\n" +
        "host3 -> DONE (3/3)\n" +
        "\n" +
        "Done.\n" +
        "Duration: 4.00 s\n");

    assertEquals(expected, output.replaceAll("\\p{Blank}+|(?:\\p{Blank})$", " "));
  }

  private static class TimeUtil implements RollingUpdateCommand.SleepFunction, Supplier<Long> {

    private long curentTimeMillis = 0;

    @Override
    public void sleep(final long millis) throws InterruptedException {
      advanceTime(millis);
    }

    public void advanceTime(final long millis) {
      curentTimeMillis += millis;
    }

    @Override
    public Long get() {
      return curentTimeMillis;
    }
  }

  private static class ResponseAnswer implements Answer<
      ListenableFuture<DeploymentGroupStatusResponse>> {
    private final List<DeploymentGroupStatusResponse> responses;
    private int index = 0;

    public ResponseAnswer(final DeploymentGroupStatusResponse... responses) {
      this(Arrays.asList(responses));
    }

    public ResponseAnswer(final List<DeploymentGroupStatusResponse> responses) {
      this.responses = responses;
    }

    @Override
    public ListenableFuture<DeploymentGroupStatusResponse> answer(
        final InvocationOnMock ignored) {
      return immediateFuture(responses.get(index++ % responses.size()));
    }
  }

  private static void assertJsonOutputEquals(
      final String actual,
      final Map<String, Object> expected) throws IOException {
    // * Long(2) != Integer(2)
    // * Json serializing a Long and then parsing it makes it into an Integer (in some cases?)
    // * => Can't easily compare a map with a json-deserialized map
    // * => Serialize and deserialize the expected value map, and compare against that
    final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
    final Map<String, Object> actualMap = Json.read(actual, typeRef);
    final Map<String, Object> expectedMap = Json.read(Json.asString(expected), typeRef);
    assertEquals(expectedMap, actualMap);
  }
}
