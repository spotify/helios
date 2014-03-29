/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import com.kpelykh.docker.client.DockerClient;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MultiplePortJobTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final Range<Integer> portRange = temporaryPorts.localPortRange("agent1", 2);
    final AgentMain agent1 = startDefaultAgent(TEST_HOST, "--port-range=" +
                                                          portRange.lowerEndpoint() + ":" +
                                                          portRange.upperEndpoint());

    final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT, false);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    final Map<String, PortMapping> ports1 =
        ImmutableMap.of("foo", PortMapping.of(4711),
                        "bar", PortMapping.of(4712, EXTERNAL_PORT1));

    final ImmutableMap<String, PortMapping> expectedMapping1 =
        ImmutableMap.of("foo", PortMapping.of(4711, portRange.lowerEndpoint()),
                        "bar", PortMapping.of(4712, EXTERNAL_PORT1));

    final Map<String, PortMapping> ports2 =
        ImmutableMap.of("foo", PortMapping.of(4711),
                        "bar", PortMapping.of(4712, EXTERNAL_PORT2));

    final ImmutableMap<String, PortMapping> expectedMapping2 =
        ImmutableMap.of("foo", PortMapping.of(4711, portRange.lowerEndpoint() + 1),
                        "bar", PortMapping.of(4712, EXTERNAL_PORT2));

    final JobId jobId1 = createJob(JOB_NAME + 1, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                   EMPTY_ENV, ports1);

    assertNotNull(jobId1);
    deployJob(jobId1, TEST_HOST);
    final TaskStatus firstTaskStatus1 = awaitJobState(client, TEST_HOST, jobId1, RUNNING,
                                                      LONG_WAIT_MINUTES, MINUTES);

    final JobId jobId2 = createJob(JOB_NAME + 2, JOB_VERSION, "busybox", DO_NOTHING_COMMAND,
                                   EMPTY_ENV, ports2);

    assertNotNull(jobId2);
    deployJob(jobId2, TEST_HOST);
    final TaskStatus firstTaskStatus2 = awaitJobState(client, TEST_HOST, jobId2, RUNNING,
                                                      LONG_WAIT_MINUTES, MINUTES);

    assertEquals(expectedMapping1, firstTaskStatus1.getPorts());
    assertEquals(expectedMapping2, firstTaskStatus2.getPorts());

    // TODO (dano): the supervisor should report the allocated ports at all times

    // Verify that port allocation is kept across container restarts
    dockerClient.kill(firstTaskStatus1.getContainerId());
    final TaskStatus restartedTaskStatus1 = Polling.await(
        LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final HostStatus hostStatus = client.hostStatus(TEST_HOST).get();
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId1);
        return (taskStatus != null && taskStatus.getState() == RUNNING &&
                !Objects.equals(taskStatus.getContainerId(), firstTaskStatus1.getContainerId()))
               ? taskStatus : null;
      }
    });
    assertEquals(expectedMapping1, restartedTaskStatus1.getPorts());

    // Verify that port allocation is kept across agent restarts
    agent1.stopAsync().awaitTerminated();
    dockerClient.kill(firstTaskStatus2.getContainerId());
    startDefaultAgent(TEST_HOST);
    final TaskStatus restartedTaskStatus2 = Polling.await(
        LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final HostStatus hostStatus = client.hostStatus(TEST_HOST).get();
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId2);
        return (taskStatus != null && taskStatus.getState() == RUNNING &&
                !Objects.equals(taskStatus.getContainerId(), firstTaskStatus2.getContainerId()))
               ? taskStatus : null;
      }
    });
    assertEquals(expectedMapping2, restartedTaskStatus2.getPorts());
  }
}
