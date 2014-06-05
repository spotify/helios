/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.Polling;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.docker.client.DockerClient.LogsParameter.STDERR;
import static com.spotify.docker.client.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class EnvironmentVariableTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(getTestHost(),
                      "--env",
                      "SPOTIFY_POD=PODNAME",
                      "SPOTIFY_ROLE=ROLENAME",
                      "BAR=badfood");
    awaitHostStatus(getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for the agent to report environment vars
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Map<String, HostStatus> status = Json.read(
            cli("hosts", getTestHost(), "--json"),
            new TypeReference<Map<String, HostStatus>>() {});
        return status.get(getTestHost()).getEnvironment();
      }
    });

    final DockerClient dockerClient = new DefaultDockerClient(DOCKER_ENDPOINT);

    final List<String> command = asList("sh", "-c",
                                        "echo pod: $SPOTIFY_POD; " +
                                        "echo role: $SPOTIFY_ROLE; " +
                                        "echo foo: $FOO; " +
                                        "echo bar: $BAR");

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", command,
                                  ImmutableMap.of("FOO", "4711",
                                                  "BAR", "deadbeef"));

    // deploy
    deployJob(jobId, getTestHost());

    final TaskStatus taskStatus = awaitTaskState(jobId, getTestHost(), EXITED);

    final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR);
    final String log = logs.readFully();

    assertContains("pod: PODNAME", log);
    assertContains("role: ROLENAME", log);
    assertContains("foo: 4711", log);

    // Verify that the the BAR environment variable in the job overrode the agent config
    assertContains("bar: deadbeef", log);

    Map<String, HostStatus> status = Json.read(cli("hosts", getTestHost(), "--json"),
                                               new TypeReference<Map<String, HostStatus>>() {});

    assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
                                 "SPOTIFY_ROLE", "ROLENAME",
                                 "BAR", "badfood"),
                 status.get(getTestHost()).getEnvironment());

    assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
                                 "SPOTIFY_ROLE", "ROLENAME",
                                 "BAR", "deadbeef",
                                 "FOO", "4711"),
                 status.get(getTestHost()).getStatuses().get(jobId).getEnv());
  }
}
