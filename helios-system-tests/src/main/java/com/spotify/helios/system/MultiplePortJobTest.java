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

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MultiplePortJobTest extends SystemTestBase {

  private final int externalPort1 = temporaryPorts.localPort("external-1");
  private final int externalPort2 = temporaryPorts.localPort("external-2");

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final Range<Integer> portRange = temporaryPorts.localPortRange("agent1", 2);
    final AgentMain agent1 = startDefaultAgent(testHost(), "--port-range=" +
                                                          portRange.lowerEndpoint() + ":" +
                                                          portRange.upperEndpoint());

    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri())) {
      final HeliosClient client = defaultClient();

      awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

      final Map<String, PortMapping> ports1 =
          ImmutableMap.of("foo", PortMapping.of(4711),
                          "bar", PortMapping.of(4712, externalPort1));

      final ImmutableMap<String, PortMapping> expectedMapping1 =
          ImmutableMap.of("foo", PortMapping.of(4711, portRange.lowerEndpoint()),
                          "bar", PortMapping.of(4712, externalPort1));

      final Map<String, PortMapping> ports2 =
          ImmutableMap.of("foo", PortMapping.of(4711),
                          "bar", PortMapping.of(4712, externalPort2));

      final ImmutableMap<String, PortMapping> expectedMapping2 =
          ImmutableMap.of("foo", PortMapping.of(4711, portRange.lowerEndpoint() + 1),
                          "bar", PortMapping.of(4712, externalPort2));

      final JobId jobId1 = createJob(testJobName + 1, testJobVersion, "busybox", IDLE_COMMAND,
                                     EMPTY_ENV, ports1);

      assertNotNull(jobId1);
      deployJob(jobId1, testHost());
      final TaskStatus firstTaskStatus1 = awaitJobState(client, testHost(), jobId1, RUNNING,
                                                        LONG_WAIT_MINUTES, MINUTES);

      final JobId jobId2 = createJob(testJobName + 2, testJobVersion, "busybox", IDLE_COMMAND,
                                     EMPTY_ENV, ports2);

      assertNotNull(jobId2);
      deployJob(jobId2, testHost());
      final TaskStatus firstTaskStatus2 = awaitJobState(client, testHost(), jobId2, RUNNING,
                                                        LONG_WAIT_MINUTES, MINUTES);

      assertEquals(expectedMapping1, firstTaskStatus1.getPorts());
      assertEquals(expectedMapping2, firstTaskStatus2.getPorts());

      // TODO (dano): the supervisor should report the allocated ports at all times

      // Verify that port allocation is kept across container restarts
      dockerClient.killContainer(firstTaskStatus1.getContainerId());
      final TaskStatus restartedTaskStatus1 = Polling.await(
          LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
        @Override
        public TaskStatus call() throws Exception {
          final HostStatus hostStatus = client.hostStatus(testHost()).get();
          final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId1);
          return (taskStatus != null && taskStatus.getState() == RUNNING &&
                  !Objects.equals(taskStatus.getContainerId(), firstTaskStatus1.getContainerId()))
                 ? taskStatus : null;
        }
      });
      assertEquals(expectedMapping1, restartedTaskStatus1.getPorts());

      // Verify that port allocation is kept across agent restarts
      agent1.stopAsync().awaitTerminated();
      dockerClient.killContainer(firstTaskStatus2.getContainerId());
      startDefaultAgent(testHost());
      final TaskStatus restartedTaskStatus2 = Polling.await(
          LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
        @Override
        public TaskStatus call() throws Exception {
          final HostStatus hostStatus = client.hostStatus(testHost()).get();
          final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId2);
          return (taskStatus != null && taskStatus.getState() == RUNNING &&
                  !Objects.equals(taskStatus.getContainerId(), firstTaskStatus2.getContainerId()))
                 ? taskStatus : null;
        }
      });
      assertEquals(expectedMapping2, restartedTaskStatus2.getPorts());
    }
  }
}
