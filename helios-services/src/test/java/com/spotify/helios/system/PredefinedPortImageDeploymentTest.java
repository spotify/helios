/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PredefinedPortImageDeploymentTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);

    final HeliosClient client = defaultClient();

    // Create a job using an image exposing port 11211 but without mapping it
    final Job job1 = Job.newBuilder()
        .setName(PREFIX + "memcached")
        .setVersion("v1")
        .setImage("skxskx/memcached")
        .setCommand(DO_NOTHING_COMMAND)
        .build();
    final JobId jobId1 = job1.getId();
    client.createJob(job1).get();

    // Create a job using an image exposing port 11211 and map it to a specific external port
    final Job job2 = Job.newBuilder()
        .setName(PREFIX + "memcached")
        .setVersion("v2")
        .setImage("skxskx/memcached")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("tcp", PortMapping.of(11211, EXTERNAL_PORT)))
        .build();
    final JobId jobId2 = job2.getId();
    client.createJob(job2).get();

    // Wait for agent to come up
    awaitHostRegistered(client, TEST_HOST, LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the jobs on the agent
    client.deploy(Deployment.of(jobId1, START), TEST_HOST).get();
    client.deploy(Deployment.of(jobId2, START), TEST_HOST).get();

    // Wait for the jobs to run
    awaitJobState(client, TEST_HOST, jobId1, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    awaitJobState(client, TEST_HOST, jobId2, RUNNING, LONG_WAIT_MINUTES, MINUTES);
  }
}
