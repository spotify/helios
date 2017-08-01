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
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.lang.Integer.toHexString;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class DeploymentTest extends SystemTestBase {

  private static final JobId BOGUS_JOB = new JobId("bogus", "job", Strings.repeat("0", 40));
  private static final String BOGUS_HOST = "BOGUS_HOST";

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void testLotsOfConcurrentJobs() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost());

    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final int numberOfJobs = 40;
    final List<JobId> jobIds = Lists.newArrayListWithCapacity(numberOfJobs);

    final String jobName = testJobName + "_" + toHexString(ThreadLocalRandom.current().nextInt());

    // create and deploy a bunch of jobs
    for (Integer i = 0; i < numberOfJobs; i++) {
      final Job job = Job.newBuilder()
          .setName(jobName)
          .setVersion(i.toString())
          .setImage(BUSYBOX)
          .setCommand(IDLE_COMMAND)
          .setCreatingUser(TEST_USER)
          .build();

      final JobId jobId = job.getId();
      final CreateJobResponse created = client.createJob(job).get();
      assertEquals(CreateJobResponse.Status.OK, created.getStatus());

      final Deployment deployment = Deployment.of(jobId, START, TEST_USER);
      final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
      assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

      jobIds.add(jobId);
    }

    // get the container ID's for the jobs
    final Set<String> containerIds = Sets.newHashSetWithExpectedSize(numberOfJobs);
    for (final JobId jobId : jobIds) {
      final TaskStatus taskStatus = awaitJobState(client, testHost(), jobId, RUNNING,
          LONG_WAIT_SECONDS, SECONDS);
      containerIds.add(taskStatus.getContainerId());
    }

    try (final DockerClient dockerClient = getNewDockerClient()) {
      // kill all the containers for the jobs
      for (final String containerId : containerIds) {
        dockerClient.killContainer(containerId);
      }

      // make sure all the containers come back up
      final int restartedContainers = Polling.await(LONG_WAIT_SECONDS, SECONDS,
          new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
              int matchingContainerCount = 0;

              for (final Container c : dockerClient.listContainers()) {
                for (final String name : c.names()) {
                  if (name.contains(jobName)) {
                    matchingContainerCount++;
                  }
                }
              }

              if (matchingContainerCount < containerIds.size()) {
                return null;
              } else {
                return matchingContainerCount;
              }
            }
          });
      assertEquals(numberOfJobs, restartedContainers);
    }
  }

  @Test
  public void test() throws Exception {
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foos", PortMapping.of(17, externalPort));

    startDefaultMaster();

    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(ports)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final CreateJobResponse duplicateJob = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.JOB_ALREADY_EXISTS, duplicateJob.getStatus());

    // Try querying for the job
    final Map<JobId, Job> noMatchJobs = client.jobs(testJobName + "not_matching").get();
    assertTrue(noMatchJobs.isEmpty());

    final Map<JobId, Job> matchJobs1 = client.jobs(testJobName).get();
    assertJobsEqual(ImmutableMap.of(jobId, job), matchJobs1);

    final Map<JobId, Job> matchJobs2 = client.jobs(testJobName + ":" + testJobVersion).get();
    assertJobsEqual(ImmutableMap.of(jobId, job), matchJobs2);

    final Map<JobId, Job> matchJobs3 = client.jobs(job.getId().toString()).get();
    assertJobsEqual(ImmutableMap.of(jobId, job), matchJobs3);

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START, TEST_USER);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    final JobDeployResponse deployed2 = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, deployed2.getStatus());

    final JobDeployResponse deployed3 = client.deploy(Deployment.of(BOGUS_JOB, START),
        testHost()).get();
    assertEquals(JobDeployResponse.Status.JOB_NOT_FOUND, deployed3.getStatus());

    final JobDeployResponse deployed4 = client.deploy(deployment, BOGUS_HOST).get();
    assertEquals(JobDeployResponse.Status.HOST_NOT_FOUND, deployed4.getStatus());

    // undeploy and redeploy to make sure things still work in the face of the tombstone
    final JobUndeployResponse undeployResp = client.undeploy(jobId, testHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployResp.getStatus());

    final JobDeployResponse redeployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, redeployed.getStatus());

    // Check that the job is in the desired state
    final Deployment fetchedDeployment = client.deployment(testHost(), jobId).get();
    assertEquals(deployment, fetchedDeployment);

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    assertJobEquals(job, taskStatus.getJob());

    assertEquals(JobDeleteResponse.Status.STILL_IN_USE, client.deleteJob(jobId).get().getStatus());

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    final HostStatus hostStatus = client.hostStatus(testHost()).get();
    taskStatus = hostStatus.getStatuses().get(jobId);
    assertEquals(jobId.toString(), RUNNING, taskStatus.getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, testHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final Deployment undeployedJob = client.deployment(testHost(), jobId).get();
    assertTrue(undeployedJob == null);

    // Wait for the task to disappear
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, client.deleteJob(jobId).get().getStatus());

    // Verify that a nonexistent job returns JOB_NOT_FOUND
    assertEquals(JobDeleteResponse.Status.JOB_NOT_FOUND, client.deleteJob(jobId).get().getStatus());
  }

  @Test
  public void testJobWithDigest() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX_WITH_DIGEST)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Try querying for the job
    final Map<JobId, Job> matchJobs = client.jobs(testJobName).get();
    assertJobsEqual(ImmutableMap.of(jobId, job), matchJobs);
    assertEquals(BUSYBOX_WITH_DIGEST, matchJobs.get(jobId).getImage());

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START, TEST_USER);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    assertJobEquals(job, taskStatus.getJob());
  }
}
