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

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.docker.client.DockerClient;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
import java.util.concurrent.Callable;
import org.junit.Test;

public class AgentRestartTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final DockerClient dockerClient = getNewDockerClient();

    final HeliosClient client = defaultClient();

    final AgentMain agent1 = startDefaultAgent(testHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, testHost(), jobId, RUNNING,
        LONG_WAIT_SECONDS, SECONDS);
    assertJobEquals(job, firstTaskStatus.getJob());
    assertEquals(1, listContainers(dockerClient, testTag).size());
    assertTrue(dockerClient.inspectContainer(firstTaskStatus.getContainerId()).state().running());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    final HostStatus hostStatus = client.hostStatus(testHost()).get();
    final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
    if (firstTaskStatus.getState() == PULLING_IMAGE) {
      final State state = taskStatus.getState();
      assertTrue(state == RUNNING || state == PULLING_IMAGE);
    } else {
      assertEquals(RUNNING, taskStatus.getState());
    }
    assertEquals(firstTaskStatus.getContainerId(), taskStatus.getContainerId());
    assertEquals(1, listContainers(dockerClient, testTag).size());
    assertTrue(dockerClient.inspectContainer(firstTaskStatus.getContainerId()).state().running());

    // Stop the agent
    agent2.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Kill the container
    dockerClient.killContainer(firstTaskStatus.getContainerId());
    assertEquals(0, listContainers(dockerClient, testTag).size());

    // Start the agent again
    final AgentMain agent3 = startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the job to be restarted in a new container
    final TaskStatus secondTaskStatus = Polling.await(
        LONG_WAIT_SECONDS, SECONDS,
        new Callable<TaskStatus>() {
          @Override
          public TaskStatus call() throws Exception {
            final HostStatus hostStatus = client.hostStatus(testHost()).get();
            final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
            return (taskStatus != null && taskStatus.getContainerId() != null
                    && taskStatus.getState() == RUNNING
                    && !taskStatus.getContainerId().equals(firstTaskStatus.getContainerId()))
                   ? taskStatus
                   : null;
          }
        });
    assertEquals(1, listContainers(dockerClient, testTag).size());
    assertTrue(dockerClient.inspectContainer(secondTaskStatus.getContainerId()).state().running());

    // Stop the agent
    agent3.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Kill and destroy the container
    dockerClient.killContainer(secondTaskStatus.getContainerId());
    removeContainer(dockerClient, secondTaskStatus.getContainerId());

    // Start the agent again
    final AgentMain agent4 = startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the task to be restarted in a new container
    final TaskStatus thirdTaskStatus = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, new Callable<TaskStatus>() {
          @Override
          public TaskStatus call() throws Exception {
            final HostStatus hostStatus = client.hostStatus(testHost()).get();
            final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
            return (taskStatus != null && taskStatus.getContainerId() != null
                    && taskStatus.getState() == RUNNING
                    && !taskStatus.getContainerId().equals(secondTaskStatus.getContainerId()))
                   ? taskStatus
                   : null;
          }
        });
    assertEquals(1, listContainers(dockerClient, testTag).size());
    assertTrue(dockerClient.inspectContainer(thirdTaskStatus.getContainerId()).state().running());

    // Stop the agent
    agent4.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Stop the job
    final SetGoalResponse stopped = client.setGoal(Deployment.of(jobId, STOP), testHost()).get();
    assertEquals(SetGoalResponse.Status.OK, stopped.getStatus());

    // Start the agent again
    final AgentMain agent5 = startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Verify that the task is stopped
    awaitJobState(client, testHost(), jobId, STOPPED, LONG_WAIT_SECONDS, SECONDS);
    assertEquals(0, listContainers(dockerClient, testTag).size());

    // Stop the agent
    agent5.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Start the job
    final SetGoalResponse started = client.setGoal(Deployment.of(jobId, START),
        testHost()).get();
    assertEquals(SetGoalResponse.Status.OK, started.getStatus());

    // Start the agent again
    final AgentMain agent6 = startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Verify that the task is started
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    assertEquals(1, listContainers(dockerClient, testTag).size());

    // Stop the agent
    agent6.stopAsync().awaitTerminated();
    awaitHostStatus(client, testHost(), DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, testHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Start the agent again
    startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the task to get removed
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);
    assertEquals(0, listContainers(dockerClient, testTag).size());
  }
}
