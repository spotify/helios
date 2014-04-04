/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class PortCollisionJobTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    final Job job1 = Job.newBuilder()
        .setName(PREFIX + "foo")
        .setVersion("1")
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10001, externalPort1)))
        .build();

    final Job job2 = Job.newBuilder()
        .setName(PREFIX + "bar")
        .setVersion("1")
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10002, externalPort1)))
        .build();

    final CreateJobResponse created1 = client.createJob(job1).get();
    assertEquals(CreateJobResponse.Status.OK, created1.getStatus());

    final CreateJobResponse created2 = client.createJob(job2).get();
    assertEquals(CreateJobResponse.Status.OK, created2.getStatus());

    final Deployment deployment1 = Deployment.of(job1.getId(), STOP);
    final JobDeployResponse deployed1 = client.deploy(deployment1, TEST_HOST).get();
    assertEquals(JobDeployResponse.Status.OK, deployed1.getStatus());

    final Deployment deployment2 = Deployment.of(job2.getId(), STOP);
    final JobDeployResponse deployed2 = client.deploy(deployment2, TEST_HOST).get();
    assertEquals(JobDeployResponse.Status.PORT_CONFLICT, deployed2.getStatus());
  }
}
