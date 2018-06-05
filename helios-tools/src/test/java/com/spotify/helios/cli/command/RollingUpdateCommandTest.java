/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli.command;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.DeploymentGroup.RollingUpdateReason.MANUAL;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class RollingUpdateCommandTest {

  private static final String GROUP_NAME = "my_group";
  private static final JobId JOB_ID = new JobId("foo", "2", "1212121212121212121");
  private static final JobId OLD_JOB_ID = new JobId("foo", "1", "3232323232323232323");
  private static final JobId NEW_JOB_ID = new JobId("foo", "3", "4242424242424242424");
  private static final int PARALLELISM = 1;
  private static final long TIMEOUT = 300;
  private static final String TOKEN = "my_token";

  private static final RolloutOptions jobOptions = RolloutOptions.newBuilder()
      .setParallelism(123)
      .setTimeout(456L)
      .setMigrate(true)
      .setOverlap(true)
      .setToken("my_token_from_job_options")
      .setIgnoreFailures(true)
      .build();

  private static final Job JOB = Job.newBuilder()
      .setName(JOB_ID.getName())
      .setVersion(JOB_ID.getVersion())
      .setHash(JOB_ID.getHash())
      .setRolloutOptions(jobOptions)
      .build();

  private static final RolloutOptions OPTIONS = RolloutOptions.newBuilder()
      .setTimeout(TIMEOUT)
      .setParallelism(PARALLELISM)
      .setMigrate(false)
      .setOverlap(false)
      .setToken(TOKEN)
      .setIgnoreFailures(false)
      .build();

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
    when(options.getString("token")).thenReturn(TOKEN);
    when(options.getBoolean("ignore_failures")).thenReturn(false);
  }

  private static DeploymentGroupStatusResponse.HostStatus makeHostStatus(
      final String host, final JobId jobId, final TaskStatus.State state) {
    return new DeploymentGroupStatusResponse.HostStatus(host, jobId, state);
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
        DeploymentGroup.newBuilder()
            .setName(GROUP_NAME)
            .setHostSelectors(Collections.<HostSelector>emptyList())
            .setJobId(jobId)
            .setRolloutOptions(RolloutOptions.getDefault())
            .setRollingUpdateReason(MANUAL)
            .build(),
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
            makeHostStatus("host2", OLD_JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", JOB_ID, TaskStatus.State.CREATING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", JOB_ID, TaskStatus.State.RUNNING))
    ));

    final int ret = command.runWithJob(options, client, out, false, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(0, ret);
  }

  @Test
  public void testRollingUpdateAsync() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(options.getBoolean("async")).thenReturn(true);

    final int ret = command.runWithJob(options, client, out, false, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(0, ret);

    final String expected =
        "Rolling update (async) started: my_group -> foo:2:1212121 (parallelism=1, timeout=300, "
        + "overlap=false, token=" + TOKEN + ", ignoreFailures=false, migrate=false)\n";

    assertEquals(expected, output);
  }

  @Test
  public void testRollingUpdateFailsIfJobIdChangedDuringRollout() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", null, null),
            makeHostStatus("host2", OLD_JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, NEW_JOB_ID, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.STARTING),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING))
    ));

    final int ret = command.runWithJob(options, client, out, false, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);
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
            makeHostStatus("host1", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJob(options, client, out, false, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);
  }

  @Test
  public void testRollingUpdateFailed() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.FAILED, "foobar",
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJob(options, client, out, false, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);
  }

  // ----------------------------

  @Test
  public void testRollingUpdateJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", JOB_ID, TaskStatus.State.RUNNING))
    ));

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(0, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "DONE")
        .put("duration", 0.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build());
  }

  @Test
  public void testRollingUpdateAsyncJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(options.getBoolean("async")).thenReturn(true);

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(0, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "OK")
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build()
    );
  }

  @Test
  public void testRollingUpdateFailsIfJobIdChangedDuringRolloutJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", null, null),
            makeHostStatus("host2", OLD_JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, NEW_JOB_ID, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", JOB_ID, TaskStatus.State.STARTING),
            makeHostStatus("host3", OLD_JOB_ID, TaskStatus.State.RUNNING))
    ));

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "FAILED")
        .put("error", "Deployment-group job id changed during rolling-update")
        .put("duration", 1.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build());
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
            makeHostStatus("host1", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "TIMEOUT")
        .put("duration", 601.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build());
  }

  @Test
  public void testRollingUpdateFailedJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.PULLING_IMAGE),
            makeHostStatus("host2", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.FAILED, "foobar",
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING),
            makeHostStatus("host2", null, null))
    ));

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, OPTIONS);
    assertEquals(1, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "FAILED")
        .put("error", "foobar")
        .put("duration", 1.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build()
    );
  }

  @Test
  public void testRollingUpdateMigrateJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));
    when(options.getBoolean("migrate")).thenReturn(true);

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    // Verify that rollingUpdate() was called with migrate=true
    final RolloutOptions rolloutOptions = RolloutOptions.newBuilder()
        .setTimeout(TIMEOUT)
        .setParallelism(PARALLELISM)
        .setMigrate(true)
        .setOverlap(false)
        .setToken(TOKEN)
        .setIgnoreFailures(false)
        .build();
    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, rolloutOptions);
    assertEquals(0, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "DONE")
        .put("duration", 0.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", false)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", true)
        .build());
  }

  @Test
  public void testRollingUpdateOverlapJson() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));
    when(options.getBoolean("overlap")).thenReturn(true);

    final int ret = command.runWithJob(options, client, out, true, JOB, null);
    final String output = baos.toString();

    // Verify that rollingUpdate() was called with migrate=true
    final RolloutOptions rolloutOptions = RolloutOptions.newBuilder()
        .setTimeout(TIMEOUT)
        .setParallelism(PARALLELISM)
        .setMigrate(false)
        .setOverlap(true)
        .setToken(TOKEN)
        .setIgnoreFailures(false)
        .build();
    verify(client).rollingUpdate(GROUP_NAME, JOB_ID, rolloutOptions);
    assertEquals(0, ret);

    assertJsonOutputEquals(output, ImmutableMap.<String, Object>builder()
        .put("status", "DONE")
        .put("duration", 0.00)
        .put("parallelism", PARALLELISM)
        .put("timeout", TIMEOUT)
        .put("overlap", true)
        .put("token", TOKEN)
        .put("ignoreFailures", false)
        .put("migrate", false)
        .build()
    );
  }

  @Test
  public void testCommandLineOptions() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));

    String optionString = "(parallelism=1, timeout=300, overlap=false, token=my_token, "
                          + "ignoreFailures=false, migrate=false)";

    command.runWithJob(options, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(optionString));
  }

  @Test
  public void testFallbackToJobOptions() throws Exception {
    final Namespace cmdlineOptions = mock(Namespace.class);
    when(cmdlineOptions.getString("deployment-group-name")).thenReturn(null);
    when(cmdlineOptions.getInt("parallelism")).thenReturn(null);
    when(cmdlineOptions.getLong("timeout")).thenReturn(null);
    when(cmdlineOptions.getLong("rollout_timeout")).thenReturn(10L);
    when(cmdlineOptions.getBoolean("async")).thenReturn(true);
    when(cmdlineOptions.getBoolean("migrate")).thenReturn(null);
    when(cmdlineOptions.getBoolean("overlap")).thenReturn(null);
    when(cmdlineOptions.getString("token")).thenReturn("cli_token");
    when(cmdlineOptions.getBoolean("ignore_failures")).thenReturn(null);

    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));

    String optionString = "(parallelism=123, timeout=456, overlap=true, "
        + "token=cli_token, ignoreFailures=true, migrate=true)";

    command.runWithJob(cmdlineOptions, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(optionString));
  }

  @Test
  public void testJobWithNoOptions() throws Exception {
    final JobId jobId = new JobId("bar", "2", "1212121212121212121");
    final Job job = Job.newBuilder()
        .setName(jobId.getName())
        .setVersion(jobId.getVersion())
        .setHash(jobId.getHash())
        .build();

    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, jobId, null,
            makeHostStatus("host1", jobId, TaskStatus.State.RUNNING))
    ));

    final int ret = command.runWithJob(options, client, out, false, job, null);

    assertThat(ret, equalTo(0));
  }

  @Test
  public void testRollingUpdateSuccessOutput() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ACTIVE, null,
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));

    when(options.getBoolean("overlap")).thenReturn(true);

    final String expectedSubstring = "host1 -> RUNNING (1/1)\n"
        + "\n"
        + "Done.\n"
        + "Duration: 0.00 s\n";

    command.runWithJob(options, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(expectedSubstring));
  }

  @Test
  public void testRollingUpdateFailureOutput() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.FAILED, "mock failure",
            makeHostStatus("host1", JOB_ID, TaskStatus.State.RUNNING))
    ));

    when(options.getBoolean("overlap")).thenReturn(true);

    final String expectedSubstring = "Failed: mock failure\n";

    command.runWithJob(options, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(expectedSubstring));
  }

  @Test
  public void testGroupIdChangedDuringRolloutOutput() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, NEW_JOB_ID, null,
            makeHostStatus("host1", null, null))
    ));

    final String expectedSubstring = "Failed: Deployment-group job id changed during "
                                     + "rolling-update";

    command.runWithJob(options, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(expectedSubstring));
  }

  @Test
  public void testTimeoutDuringRolloutOutput() throws Exception {
    when(client.rollingUpdate(anyString(), any(JobId.class), any(RolloutOptions.class)))
        .thenReturn(immediateFuture(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)));

    when(client.deploymentGroupStatus(GROUP_NAME)).then(new ResponseAnswer(
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", null, null)),
        statusResponse(DeploymentGroupStatusResponse.Status.ROLLING_OUT, null,
            makeHostStatus("host1", null, null))
    ));

    final String expectedSubstring = "Timed out! (rolling-update still in progress)";

    command.runWithJob(options, client, out, false, JOB, null);

    assertThat(baos.toString(), containsString(expectedSubstring));
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
