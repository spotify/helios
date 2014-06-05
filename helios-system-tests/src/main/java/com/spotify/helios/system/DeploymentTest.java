/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
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

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeploymentTest extends SystemTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void test() throws Exception {
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foos", PortMapping.of(17, externalPort));

    startDefaultMaster();

    final HeliosClient client = defaultClient();
    startDefaultAgent(getTestHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(JOB_VERSION)
        .setImage("ubuntu:12.04")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ports)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final CreateJobResponse duplicateJob = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.JOB_ALREADY_EXISTS, duplicateJob.getStatus());

    // Try querying for the job
    final Map<JobId, Job> noMatchJobs = client.jobs(jobName + "not_matching").get();
    assertTrue(noMatchJobs.isEmpty());

    final Map<JobId, Job> matchJobs1 = client.jobs(jobName).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs1);

    final Map<JobId, Job> matchJobs2 = client.jobs(jobName + ":" + JOB_VERSION).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs2);

    final Map<JobId, Job> matchJobs3 = client.jobs(job.getId().toString()).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs3);

    // Wait for agent to come up
    awaitHostRegistered(client, getTestHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    final JobDeployResponse deployed2 = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, deployed2.getStatus());

    final JobDeployResponse deployed3 = client.deploy(Deployment.of(BOGUS_JOB, START),
                                                      getTestHost()).get();
    assertEquals(JobDeployResponse.Status.JOB_NOT_FOUND, deployed3.getStatus());

    final JobDeployResponse deployed4 = client.deploy(deployment, BOGUS_HOST).get();
    assertEquals(JobDeployResponse.Status.HOST_NOT_FOUND, deployed4.getStatus());

    // undeploy and redeploy to make sure things still work in the face of the tombstone
    JobUndeployResponse undeployResp = client.undeploy(jobId, getTestHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployResp.getStatus());

    final JobDeployResponse redeployed = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.OK, redeployed.getStatus());

    // Check that the job is in the desired state
    final Deployment fetchedDeployment = client.deployment(getTestHost(), jobId).get();
    assertEquals(deployment, fetchedDeployment);

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(client, getTestHost(), jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, taskStatus.getJob());

    assertEquals(JobDeleteResponse.Status.STILL_IN_USE, client.deleteJob(jobId).get().getStatus());

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    final HostStatus hostStatus = client.hostStatus(getTestHost()).get();
    taskStatus = hostStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, getTestHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final Deployment undeployedJob = client.deployment(getTestHost(), jobId).get();
    assertTrue(undeployedJob == null || undeployedJob.getGoal() == Goal.UNDEPLOY);

    // Wait for the task to disappear
    awaitTaskGone(client, getTestHost(), jobId, LONG_WAIT_MINUTES, MINUTES);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, client.deleteJob(jobId).get().getStatus());
  }
}
