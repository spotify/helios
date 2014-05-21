/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.agent.docker.DefaultDockerClient;
import com.spotify.helios.agent.docker.DockerClient;
import com.spotify.helios.agent.docker.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.List;

import static com.spotify.helios.agent.docker.DockerClient.LogsParameter.STDERR;
import static com.spotify.helios.agent.docker.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ContainerHostNameTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(getTestHost());
    awaitHostStatus(getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    final DockerClient dockerClient = new DefaultDockerClient(DOCKER_ENDPOINT);

    final List<String> command = asList("hostname", "-f");

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", command);

    // deploy
    deployJob(jobId, getTestHost());

    final TaskStatus taskStatus = awaitTaskState(jobId, getTestHost(), EXITED);

    final String log;
    try (final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
      log = logs.readFully();
    }

    assertContains(JOB_NAME + "_" + JOB_VERSION + "." + getTestHost(), log);
  }
}
