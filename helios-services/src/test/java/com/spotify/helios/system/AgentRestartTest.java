/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AgentRestartTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT);

    final HeliosClient client = defaultClient();

    final AgentMain agent1 = startDefaultAgent(TEST_AGENT);

    // A simple netcat echo server
    final List<String> command =
        asList("bash", "-c",
               "DEBIAN_FRONTEND=noninteractive " +
               "apt-get install -q -y --force-yes nmap && " +
               "ncat -l -p 4711 -c \"while true; do read i && echo $i; done\"");

    // TODO (dano): connect to the server during the test and verify that the connection is never broken

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
    awaitAgentRegistered(client, TEST_AGENT, LONG_WAIT_MINUTES, MINUTES);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, TEST_AGENT, jobId, RUNNING,
                                                     LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, firstTaskStatus.getJob());
    assertEquals(1, listContainers(dockerClient, PREFIX).size());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = client.agentStatus(TEST_AGENT).get();
    final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());
    assertEquals(firstTaskStatus.getContainerId(), taskStatus.getContainerId());

    // Stop the agent
    agent2.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Kill the container
    dockerClient.stopContainer(firstTaskStatus.getContainerId());
    assertEquals(0, listContainers(dockerClient, PREFIX).size());

    // Start the agent again
    final AgentMain agent3 = startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for the job to be restarted in a new container
    final TaskStatus secondTaskStatus = await(
        LONG_WAIT_MINUTES, MINUTES,
        new Callable<TaskStatus>() {
          @Override
          public TaskStatus call() throws Exception {
            final AgentStatus agentStatus = client.agentStatus(TEST_AGENT).get();
            final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
            return (taskStatus != null && taskStatus.getContainerId() != null &&
                    taskStatus.getState() == RUNNING &&
                    !taskStatus.getContainerId().equals(firstTaskStatus.getContainerId()))
                   ? taskStatus
                   : null;
          }
        });
    assertEquals(1, listContainers(dockerClient, PREFIX).size());

    // Stop the agent
    agent3.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Kill and destroy the container
    dockerClient.stopContainer(secondTaskStatus.getContainerId());
    dockerClient.removeContainer(secondTaskStatus.getContainerId());
    try {
      // This should fail with an exception if the container still exists
      dockerClient.inspectContainer(secondTaskStatus.getContainerId());
      fail();
    } catch (DockerException ignore) {
    }

    // Start the agent again
    final AgentMain agent4 = startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for the task to be restarted in a new container
    await(LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(TEST_AGENT).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getContainerId() != null &&
                taskStatus.getState() == RUNNING &&
                !taskStatus.getContainerId().equals(secondTaskStatus.getContainerId())) ? taskStatus
                                                                                        : null;
      }
    });
    assertEquals(1, listContainers(dockerClient, PREFIX).size());

    // Stop the agent
    agent4.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Stop the job
    final SetGoalResponse stopped = client.setGoal(Deployment.of(jobId, STOP), TEST_AGENT).get();
    assertEquals(SetGoalResponse.Status.OK, stopped.getStatus());

    // Start the agent again
    final AgentMain agent5 = startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Verify that the task is stopped
    awaitJobState(client, TEST_AGENT, jobId, STOPPED, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(0, listContainers(dockerClient, PREFIX).size());

    // Stop the agent
    agent5.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Start the job
    final SetGoalResponse started = client.setGoal(Deployment.of(jobId, START), TEST_AGENT).get();
    assertEquals(SetGoalResponse.Status.OK, started.getStatus());

    // Start the agent again
    final AgentMain agent6 = startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Verify that the task is started
    awaitJobState(client, TEST_AGENT, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(1, listContainers(dockerClient, PREFIX).size());

    // Stop the agent
    agent6.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, TEST_AGENT).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Start the agent again
    startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for the task to get removed
    awaitTaskGone(client, TEST_AGENT, jobId, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(0, listContainers(dockerClient, PREFIX).size());
  }

}
