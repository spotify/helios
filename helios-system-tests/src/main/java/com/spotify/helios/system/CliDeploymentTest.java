/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang.StringUtils.strip;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CliDeploymentTest extends SystemTestBase {
  private static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};

  private final Integer externalPort = getTemporaryPorts().localPort("external");

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });

    startDefaultAgent(getTestHost());

    final String image = "busybox";
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "hm"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");

    // Wait for agent to come up
    awaitHostRegistered(getTestHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, image, DO_NOTHING_COMMAND, env, ports,
                                  registration);

    // Query for job
    final Job expected = Job.newBuilder()
        .setName(JOB_NAME)
        .setVersion(JOB_VERSION)
        .setImage(image)
        .setCommand(DO_NOTHING_COMMAND)
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .build();
    final String inspectOutput = cli("inspect", "--json", expected.getId().toString());
    final Job parsed = Json.read(inspectOutput, Job.class);
    assertEquals(expected, parsed);
    assertContains(jobId.toString(), cli("jobs", JOB_NAME, "-q"));
    assertContains(jobId.toString(), cli("jobs", JOB_NAME + ":" + JOB_VERSION, "-q"));
    assertTrue(cli("jobs", "foozbarz", "-q").trim().isEmpty());

    // Create a new job using the first job as a template
    final Job expectedCloned = expected.toBuilder()
        .setVersion(expected.getId().getVersion() + "-cloned")
        .build();
    final JobId clonedJobId = JobId.parse(strip(cli("create", "-q", "-t",
                                                    JOB_NAME + ":" + JOB_VERSION,
                                                    JOB_NAME, JOB_VERSION + "-cloned")));
    final String clonedInspectOutput = cli("inspect", "--json", clonedJobId.toString());
    final Job clonedParsed = Json.read(clonedInspectOutput, Job.class);
    assertEquals(expectedCloned, clonedParsed);

    // Verify that port mapping and environment variables are correct
    final String statusString = cli("status", "--job", jobId.toString(), "--json");
    final Map<JobId, JobStatus> statuses = Json.read(statusString, STATUSES_TYPE);
    final Job job = statuses.get(jobId).getJob();
    assertEquals(ServicePorts.of("foo"),
                 job.getRegistration().get(ServiceEndpoint.of("foo-service", "hm")));
    assertEquals(ServicePorts.of("bar"),
                 job.getRegistration().get(ServiceEndpoint.of("bar-service", "http")));
    assertEquals(4711, job.getPorts().get("foo").getInternalPort());
    assertEquals(PortMapping.of(5000, externalPort), job.getPorts().get("bar"));
    assertEquals("f00d", job.getEnv().get("BAD"));

    final String duplicateJob = cli("create", JOB_NAME, JOB_VERSION, image, "--",
                                    DO_NOTHING_COMMAND);
    assertContains("JOB_ALREADY_EXISTS", duplicateJob);

    final String prestop = stopJob(jobId, getTestHost());
    assertContains("JOB_NOT_DEPLOYED", prestop);

    // Deploy job
    deployJob(jobId, getTestHost());

    // Stop job
    final String stop1 = stopJob(jobId, BOGUS_HOST);
    assertContains("HOST_NOT_FOUND", stop1);
    final String stop2 = stopJob(BOGUS_JOB, getTestHost());
    assertContains("Unknown job", stop2);
    final String stop3 = stopJob(jobId, getTestHost());
    assertContains(getTestHost() + ": done", stop3);

    // Verify that undeploying the job from a nonexistent host fails
    assertContains("HOST_NOT_FOUND", cli("undeploy", jobId.toString(), BOGUS_HOST));

    // Verify that undeploying a nonexistent job from the host fails
    assertContains("Unknown job", cli("undeploy", BOGUS_JOB.toString(), getTestHost()));

    // Undeploy job
    undeployJob(jobId, getTestHost());
  }
}
