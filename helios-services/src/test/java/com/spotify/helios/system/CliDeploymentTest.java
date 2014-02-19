/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.protocol.JobStatus;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CliDeploymentTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    String output = cli("master", "list");
    assertContains(TEST_MASTER, output);

    assertContains("NOT_FOUND", deregisterHost(TEST_HOST));

    startDefaultAgent(TEST_HOST);

    final String image = "busybox";
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, EXTERNAL_PORT1));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "hm"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");

    // Wait for agent to come up
    awaitHostRegistered(TEST_HOST, LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, image, DO_NOTHING_COMMAND, env, ports,
                                  registration);

    // Query for job
    assertContains(jobId.toString(), cli("job", "list", JOB_NAME, "-q"));
    assertContains(jobId.toString(), cli("job", "list", JOB_NAME + ":" + JOB_VERSION, "-q"));
    assertTrue(cli("job", "list", "foozbarz", "-q").trim().isEmpty());

    // Verify that port mapping and environment variables are correct
    final String statusString = cli("job", "status", jobId.toString(), "--json");
    final Map<JobId, JobStatus> statuses = Json.read(statusString, STATUSES_TYPE);
    final Job job = statuses.get(jobId).getJob();
    assertEquals(ServicePorts.of("foo"),
                 job.getRegistration().get(ServiceEndpoint.of("foo-service", "hm")));
    assertEquals(ServicePorts.of("bar"),
                 job.getRegistration().get(ServiceEndpoint.of("bar-service", "http")));
    assertEquals(4711, job.getPorts().get("foo").getInternalPort());
    assertEquals(PortMapping.of(5000, EXTERNAL_PORT1), job.getPorts().get("bar"));
    assertEquals("f00d", job.getEnv().get("BAD"));

    final String duplicateJob = cli(
        "job", "create", JOB_NAME, JOB_VERSION, image, "--", DO_NOTHING_COMMAND);
    assertContains("JOB_ALREADY_EXISTS", duplicateJob);

    final String prestop = stopJob(jobId, TEST_HOST);
    assertContains("JOB_NOT_DEPLOYED", prestop);

    // Deploy job
    deployJob(jobId, TEST_HOST);

    // Stop job
    final String stop1 = stopJob(jobId, BOGUS_HOST);
    assertContains("HOST_NOT_FOUND", stop1);
    final String stop2 = stopJob(BOGUS_JOB, TEST_HOST);
    assertContains("Unknown job", stop2);
    final String stop3 = stopJob(jobId, TEST_HOST);
    assertContains(TEST_HOST + ": done", stop3);

    // Verify that undeploying the job from a nonexistent host fails
    assertContains("HOST_NOT_FOUND", cli("job", "undeploy", jobId.toString(), BOGUS_HOST));

    // Verify that undeploying a nonexistent job from the host fails
    assertContains("Unknown job", cli("job", "undeploy", BOGUS_JOB.toString(), TEST_HOST));

    // Undeploy job
    undeployJob(jobId, TEST_HOST);

    assertContains(TEST_HOST + ": done", deregisterHost(TEST_HOST));
  }
}
