/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.Test;

public class EnvironmentVariableTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost(),
        "--env",
        "SPOTIFY_POD=PODNAME",
        "SPOTIFY_ROLE=ROLENAME",
        "BAR=badfood");
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the agent to report environment vars
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final Map<String, HostStatus> status = Json.read(
            cli("hosts", testHost(), "--json"),
            new TypeReference<Map<String, HostStatus>>() {});
        return status.get(testHost()).getEnvironment();
      }
    });

    try (final DockerClient dockerClient = getNewDockerClient()) {
      final List<String> command = asList("sh", "-c",
          "echo pod: $SPOTIFY_POD; "
          + "echo role: $SPOTIFY_ROLE; "
          + "echo foo: $FOO; "
          + "echo bar: $BAR");

      // Create job
      final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, command,
          ImmutableMap.of("FOO", "4711",
              "BAR", "deadbeef"));

      // deploy
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), stdout(), stderr());
      final String log = logs.readFully();

      assertThat(log, containsString("pod: PODNAME"));
      assertThat(log, containsString("role: ROLENAME"));
      assertThat(log, containsString("foo: 4711"));

      // Verify that the the BAR environment variable in the job overrode the agent config
      assertThat(log, containsString("bar: deadbeef"));

      final Map<String, HostStatus> status = Json.read(
          cli("hosts", testHost(), "--json"), new TypeReference<Map<String, HostStatus>>() {});

      assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
          "SPOTIFY_ROLE", "ROLENAME",
          "BAR", "badfood"),
          status.get(testHost()).getEnvironment());

      assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
          "SPOTIFY_ROLE", "ROLENAME",
          "BAR", "deadbeef",
          "FOO", "4711"),
          status.get(testHost()).getStatuses().get(jobId).getEnv());
    }
  }
}
