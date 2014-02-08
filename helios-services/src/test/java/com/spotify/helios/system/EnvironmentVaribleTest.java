/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kpelykh.docker.client.DockerClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.sun.jersey.api.client.ClientResponse;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class EnvironmentVaribleTest extends SystemTestBase {

  @Test
  public void testEnvVariables() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_AGENT,
                      "--env",
                      "SPOTIFY_POD=PODNAME",
                      "SPOTIFY_ROLE=ROLENAME",
                      "BAR=badfood");
    awaitAgentStatus(TEST_AGENT, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for the agent to report environment vars
    await(WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Map<String, AgentStatus> status = Json.read(
            cli("host", "status", TEST_AGENT, "--json"),
            new TypeReference<Map<String, AgentStatus>>() {});
        return status.get(TEST_AGENT).getEnvironment();
      }
    });

    final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT);

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
    deployJob(jobId, TEST_AGENT);

    final TaskStatus taskStatus = awaitTaskState(jobId, TEST_AGENT, EXITED);

    final ClientResponse response = dockerClient.logContainer(taskStatus.getContainerId());
    final String logMessage = readLogFully(response);

    assertContains("pod: PODNAME", logMessage);
    assertContains("role: ROLENAME", logMessage);
    assertContains("foo: 4711", logMessage);

    // Verify that the the BAR environment variable in the job overrode the agent config
    assertContains("bar: deadbeef", logMessage);

    Map<String, AgentStatus> status = Json.read(cli("host", "status", TEST_AGENT, "--json"),
                                                new TypeReference<Map<String, AgentStatus>>() {});

    assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
                                 "SPOTIFY_ROLE", "ROLENAME",
                                 "BAR", "badfood"),
                 status.get(TEST_AGENT).getEnvironment());

    assertEquals(ImmutableMap.of("SPOTIFY_POD", "PODNAME",
                                 "SPOTIFY_ROLE", "ROLENAME",
                                 "BAR", "deadbeef",
                                 "FOO", "4711"),
                 status.get(TEST_AGENT).getStatuses().get(jobId).getEnv());
  }
}
