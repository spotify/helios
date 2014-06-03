/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Rule;
import org.junit.Test;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class DeregisterTest extends SystemTestBase {

  @Rule public final TemporaryPorts ports = new TemporaryPorts();

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    final String host = getTestHost();
    final AgentMain agent = startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    // Create a job
    final Job job = Job.newBuilder()
        .setName(JOB_NAME)
        .setVersion(JOB_VERSION)
        .setImage("ubuntu:12.04")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(4711),
                                  "bar", PortMapping.of(4712, ports.localPort("bar"))))
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, host, LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, host, UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, host).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    awaitJobState(client, host, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);

    // Kill off agent
    agent.stopAsync().awaitTerminated();

    // Deregister agent
    final HostDeregisterResponse deregisterResponse = client.deregisterHost(host).get();
    assertEquals(HostDeregisterResponse.Status.OK, deregisterResponse.getStatus());

    // Verify that it's possible to remove the job
    final JobDeleteResponse deleteResponse = client.deleteJob(jobId).get();
    assertEquals(JobDeleteResponse.Status.OK, deleteResponse.getStatus());
  }
}
