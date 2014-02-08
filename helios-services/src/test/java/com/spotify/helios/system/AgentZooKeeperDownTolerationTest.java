/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.model.Container;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;

import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AgentZooKeeperDownTolerationTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT);

    final Client client = defaultClient();

    final AgentMain agent1 = startDefaultAgent(TEST_AGENT);

    // A simple netcat echo server
    final List<String> command =
        asList("bash", "-c",
               "DEBIAN_FRONTEND=noninteractive " +
               "apt-get install -q -y --force-yes nmap && " +
               "ncat -l -p 4711 -c \"while true; do read i && echo $i; done\"");

    // Create a job
    final Job job = Job.newBuilder()
        .setName(JOB_NAME)
        .setVersion(JOB_VERSION)
        .setImage("ubuntu:12.04")
        .setCommand(command)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitAgentRegistered(client, TEST_AGENT, WAIT_TIMEOUT_SECONDS, SECONDS);
    awaitAgentStatus(client, TEST_AGENT, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, TEST_AGENT, jobId, RUNNING,
                                                     LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, firstTaskStatus.getJob());
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Stop zookeeper
    stopZookeeper();

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Stop the agent
    agent1.stopAsync().awaitTerminated();

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(TEST_AGENT);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Kill the container
    dockerClient.stopContainer(firstTaskStatus.getContainerId());
    assertEquals(0, listContainers(dockerClient, PREFIX).size());

    // Wait for a while and make sure that a new container was spawned
    final String firstRestartedContainerId = await(30, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final List<Container> containers = listContainers(dockerClient, PREFIX);
        return containers.size() == 1 ? containers.get(0).id : null;
      }
    });
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Stop the agent
    agent2.stopAsync().awaitTerminated();

    // Kill the container
    dockerClient.stopContainer(firstRestartedContainerId);
    assertEquals(0, listContainers(dockerClient, PREFIX).size());

    // Start the agent again
    startDefaultAgent(TEST_AGENT);

    // Wait for a while and make sure that a new container was spawned
    Thread.sleep(5000);
    final String secondRestartedContainerId = await(30, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final List<Container> containers = listContainers(dockerClient, PREFIX);
        return containers.size() == 1 ? containers.get(0).id : null;
      }
    });
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Start zookeeper
    startZookeeper();

    // Verify that the agent is listed as up
    awaitAgentStatus(client, TEST_AGENT, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for the new container id to be reflected in the task status
    await(WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final JobStatus jobStatus = client.jobStatus(jobId).get();
        final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(TEST_AGENT);
        return taskStatus != null && Objects.equals(taskStatus.getContainerId(),
                                                    secondRestartedContainerId)
               ? taskStatus : null;
      }
    });
  }
}
