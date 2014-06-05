/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AgentZooKeeperDownTolerationTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final DockerClient dockerClient = new DefaultDockerClient(DOCKER_ENDPOINT);

    final HeliosClient client = defaultClient();

    final AgentMain agent1 = startDefaultAgent(getTestHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(JOB_NAME)
        .setVersion(JOB_VERSION)
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, getTestHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, getTestHost(), jobId, RUNNING,
                                                     LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, firstTaskStatus.getJob());
    assertNotNull(dockerClient.inspectContainer(firstTaskStatus.getContainerId()));

    // Stop zookeeper
    zk.stop();

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    assertTrue(
        dockerClient.inspectContainer(firstTaskStatus.getContainerId()).state().running());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(getTestHost());

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    assertTrue(
        dockerClient.inspectContainer(firstTaskStatus.getContainerId()).state().running());

    // Kill the container
    dockerClient.killContainer(firstTaskStatus.getContainerId());
    assertFalse(
        dockerClient.inspectContainer(firstTaskStatus.getContainerId()).state().running());

    // Wait for a while and make sure that a new container was spawned
    final String firstRestartedContainerId =
        Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
          @Override
          public String call() throws Exception {
            final List<Container> containers = listContainers(dockerClient, PREFIX);
            return containers.size() == 1 ? containers.get(0).id() : null;
          }
        });

    // Stop the agent
    agent2.stopAsync().awaitTerminated();

    // Kill the container
    dockerClient.killContainer(firstRestartedContainerId);
    assertFalse(dockerClient.inspectContainer(firstRestartedContainerId).state().running());

    // Start the agent again
    startDefaultAgent(getTestHost());

    // Wait for a while and make sure that a new container was spawned
    final String secondRestartedContainerId =
        Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
          @Override
          public String call() throws Exception {
            final List<Container> containers = listContainers(dockerClient, PREFIX);
            return containers.size() == 1 ? containers.get(0).id() : null;
          }
        });
    assertTrue(dockerClient.inspectContainer(secondRestartedContainerId).state().running());

    // Start zookeeper
    zk.start();

    // Verify that the agent is listed as up
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for the new container id to be reflected in the task status
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final JobStatus jobStatus = client.jobStatus(jobId).get();
        final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(getTestHost());
        return taskStatus != null && Objects.equals(taskStatus.getContainerId(),
                                                    secondRestartedContainerId)
               ? taskStatus : null;
      }
    });
  }
}
