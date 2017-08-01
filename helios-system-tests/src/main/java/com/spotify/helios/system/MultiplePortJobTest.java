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
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.junit.Test;

public class MultiplePortJobTest extends SystemTestBase {

  private final int externalPort1 = temporaryPorts.localPort("external-1");
  private final int externalPort2 = temporaryPorts.localPort("external-2");
  private final PortMapping staticMapping1 = PortMapping.of(4712, externalPort1);
  private final PortMapping staticMapping2 = PortMapping.of(4712, externalPort2);

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    // 'Reserve' a 2 port wide range of ports.
    final Range<Integer> portRange = temporaryPorts.localPortRange("agent1", 2);

    // Start an agent using the aforementioned 2 port wide range.
    final AgentMain agent1 = startDefaultAgent(testHost(), "--port-range="
                                                           + portRange.lowerEndpoint() + ":"
                                                           + portRange.upperEndpoint());

    try (final DockerClient dockerClient = getNewDockerClient()) {
      final HeliosClient client = defaultClient();

      awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

      // foo is a mapping of 4711 -> A port dynamically allocated by the agent's PortAllocator
      // bar is a mapping of 4712 -> A static port randomly selected by temporaryPorts
      final Map<String, PortMapping> ports1 = ImmutableMap.of("foo", PortMapping.of(4711),
          "bar", staticMapping1);

      // foo is a mapping of 4711 -> A port dynamically allocated by the agent's PortAllocator
      // bar is a mapping of 4712 -> A static port randomly selected by temporaryPorts
      final Map<String, PortMapping> ports2 = ImmutableMap.of("foo", PortMapping.of(4711),
          "bar", staticMapping2);

      final JobId jobId1 = createJob(testJobName + 1, testJobVersion, BUSYBOX, IDLE_COMMAND,
          EMPTY_ENV, ports1);
      assertNotNull(jobId1);
      deployJob(jobId1, testHost());
      final TaskStatus firstTaskStatus1 = awaitJobState(client, testHost(), jobId1, RUNNING,
          LONG_WAIT_SECONDS, SECONDS);

      final JobId jobId2 = createJob(testJobName + 2, testJobVersion, BUSYBOX, IDLE_COMMAND,
          EMPTY_ENV, ports2);
      assertNotNull(jobId2);
      deployJob(jobId2, testHost());
      final TaskStatus firstTaskStatus2 = awaitJobState(client, testHost(), jobId2, RUNNING,
          LONG_WAIT_SECONDS, SECONDS);

      // Verify we allocated dynamic ports from within the specified range.
      assertTrue(portRange.contains(firstTaskStatus1.getPorts().get("foo").getExternalPort()));
      assertTrue(portRange.contains(firstTaskStatus2.getPorts().get("foo").getExternalPort()));

      // Verify we allocated the static ports we asked for.
      assertEquals(staticMapping1, firstTaskStatus1.getPorts().get("bar"));
      assertEquals(staticMapping2, firstTaskStatus2.getPorts().get("bar"));

      // Verify we didn't allocate the same dynamic port to both jobs.
      assertNotEquals(firstTaskStatus1.getPorts().get("foo"),
          firstTaskStatus2.getPorts().get("foo"));

      // TODO (dano): the supervisor should report the allocated ports at all times

      // Verify that port allocation is kept across container restarts
      dockerClient.killContainer(firstTaskStatus1.getContainerId());
      final TaskStatus restartedTaskStatus1 = Polling.await(
          LONG_WAIT_SECONDS, SECONDS, new Callable<TaskStatus>() {
            @Override
            public TaskStatus call() throws Exception {
              final HostStatus hostStatus = client.hostStatus(testHost()).get();
              final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId1);
              return (taskStatus != null && taskStatus.getState() == RUNNING
                      && !Objects.equals(taskStatus.getContainerId(),
                  firstTaskStatus1.getContainerId()))
                     ? taskStatus : null;
            }
          });
      assertEquals(firstTaskStatus1.getPorts(), restartedTaskStatus1.getPorts());

      // Verify that port allocation is kept across agent restarts
      agent1.stopAsync().awaitTerminated();
      dockerClient.killContainer(firstTaskStatus2.getContainerId());
      startDefaultAgent(testHost());
      final TaskStatus restartedTaskStatus2 = Polling.await(
          LONG_WAIT_SECONDS, SECONDS, new Callable<TaskStatus>() {
            @Override
            public TaskStatus call() throws Exception {
              final HostStatus hostStatus = client.hostStatus(testHost()).get();
              final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId2);
              return (taskStatus != null && taskStatus.getState() == RUNNING
                      && !Objects.equals(taskStatus.getContainerId(),
                  firstTaskStatus2.getContainerId()))
                     ? taskStatus : null;
            }
          });
      assertEquals(firstTaskStatus2.getPorts(), restartedTaskStatus2.getPorts());
    }
  }

  @Test
  public void testPortEnvVars() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final Map<String, PortMapping> ports =
        ImmutableMap.of("bar", staticMapping1);

    try (final DockerClient dockerClient = getNewDockerClient()) {
      final JobId jobId = createJob(testJobName + 1, testJobVersion, BUSYBOX,
          asList("sh", "-c", "echo $HELIOS_PORT_bar"), EMPTY_ENV, ports);

      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (final LogStream logs = dockerClient.logs(taskStatus.getContainerId(),
          stdout(), stderr())) {
        log = logs.readFully();
      }
      assertEquals(testHost() + ":" + externalPort1, log.trim());
    }
  }
}
