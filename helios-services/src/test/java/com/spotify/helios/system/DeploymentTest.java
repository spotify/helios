/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeploymentTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foos", PortMapping.of(17, EXTERNAL_PORT));

    startDefaultMaster();

    final Client client = defaultClient();

    AgentStatus v = client.agentStatus(TEST_AGENT).get();
    assertNull(v); // for NOT_FOUND

    final AgentMain agent = startDefaultAgent(TEST_AGENT);

    // Create a job
    final Job job = Job.newBuilder()
        .setName(JOB_NAME)
        .setVersion(JOB_VERSION)
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ports)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final CreateJobResponse duplicateJob = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.JOB_ALREADY_EXISTS, duplicateJob.getStatus());

    // Try querying for the job
    final Map<JobId, Job> noMatchJobs = client.jobs(JOB_NAME + "not_matching").get();
    assertTrue(noMatchJobs.isEmpty());

    final Map<JobId, Job> matchJobs1 = client.jobs(JOB_NAME).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs1);

    final Map<JobId, Job> matchJobs2 = client.jobs(JOB_NAME + ":" + JOB_VERSION).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs2);

    final Map<JobId, Job> matchJobs3 = client.jobs(job.getId().toString()).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs3);

    // Wait for agent to come up
    awaitAgentRegistered(client, TEST_AGENT, LONG_WAIT_MINUTES, MINUTES);
    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    final JobDeployResponse deployed2 = client.deploy(deployment, TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, deployed2.getStatus());

    final JobDeployResponse deployed3 = client.deploy(Deployment.of(BOGUS_JOB, START),
                                                      TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.JOB_NOT_FOUND, deployed3.getStatus());

    final JobDeployResponse deployed4 = client.deploy(deployment, BOGUS_AGENT).get();
    assertEquals(JobDeployResponse.Status.AGENT_NOT_FOUND, deployed4.getStatus());

    // undeploy and redeploy to make sure things still work in the face of the tombstone
    JobUndeployResponse undeployResp = client.undeploy(jobId, TEST_AGENT).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployResp.getStatus());

    final JobDeployResponse redeployed = client.deploy(deployment, TEST_AGENT).get();
    assertEquals(JobDeployResponse.Status.OK, redeployed.getStatus());

    // Check that the job is in the desired state
    final Deployment fetchedDeployment = client.stat(TEST_AGENT, jobId).get();
    assertEquals(deployment, fetchedDeployment);

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(client, TEST_AGENT, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, taskStatus.getJob());

    assertEquals(JobDeleteResponse.Status.STILL_IN_USE, client.deleteJob(jobId).get().getStatus());

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = client.agentStatus(TEST_AGENT).get();
    taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, TEST_AGENT).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final Deployment undeployedJob = client.stat(TEST_AGENT, jobId).get();
    assertNull(undeployedJob);

    // Wait for the task to disappear
    awaitTaskGone(client, TEST_AGENT, jobId, LONG_WAIT_MINUTES, MINUTES);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, client.deleteJob(jobId).get().getStatus());

    // Stop agent and verify that the agent status changes to DOWN
    agent.stopAsync().awaitTerminated();
    awaitAgentStatus(client, TEST_AGENT, DOWN, LONG_WAIT_MINUTES, MINUTES);
  }

}
