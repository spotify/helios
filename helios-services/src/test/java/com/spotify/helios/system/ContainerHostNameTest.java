/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.kpelykh.docker.client.DockerClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.sun.jersey.api.client.ClientResponse;

import org.junit.Test;

import java.util.List;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ContainerHostNameTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);
    awaitHostStatus(TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT);

    final List<String> command = asList("hostname", "-f");

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", command);

    // deploy
    deployJob(jobId, TEST_HOST);

    final TaskStatus taskStatus = awaitTaskState(jobId, TEST_HOST, EXITED);

    final ClientResponse response = dockerClient.logContainer(taskStatus.getContainerId());
    final String logMessage = readLogFully(response);

    assertContains(JOB_NAME + "_" + JOB_VERSION + "." + TEST_HOST, logMessage);
  }
}
