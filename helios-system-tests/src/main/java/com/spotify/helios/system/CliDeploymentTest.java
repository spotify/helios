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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.ExternalPort;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class CliDeploymentTest extends SystemTestBase {

  private static final JobId BOGUS_JOB = new JobId("bogus", "job", Strings.repeat("0", 40));
  private static final String BOGUS_HOST = "BOGUS_HOST";


  private static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};

  private final ExternalPort externalPort = temporaryPorts().localExternalPort("external");

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

    startDefaultAgent(testHost());

    final String image = "busybox";
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "hm"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");

    // Wait for agent to come up
    awaitHostRegistered(testHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Create job
    final JobId jobId = createJob(testJobName, testJobVersion, image, IDLE_COMMAND, env, ports,
                                  registration);

    // Query for job
    final Job expected = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(image)
        .setCommand(IDLE_COMMAND)
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .build();
    final String inspectOutput = cli("inspect", "--json", expected.getId().toString());
    final Job parsed = Json.read(inspectOutput, Job.class);
    assertEquals(expected, parsed);
    assertThat(cli("jobs", testJobName, "-q"), containsString(jobId.toString()));
    assertThat(cli("jobs", testJobName + ":" + testJobVersion, "-q"),
               containsString(jobId.toString()));
    assertEquals("job pattern foozbarz matched no jobs", cli("jobs", "foozbarz", "-q").trim());

    // Create a new job using the first job as a template
    final Job expectedCloned = expected.toBuilder()
        .setVersion(expected.getId().getVersion() + "-cloned")
        .build();
    final JobId clonedJobId = JobId.parse(WHITESPACE.trimFrom(
        cli("create", "-q", "-t",
            testJobName + ":" + testJobVersion,
            testJobName + ":" + testJobVersion + "-cloned")));
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

    final String duplicateJob = cli("create", testJobName + ":" + testJobVersion, image, "--",
                                    IDLE_COMMAND);
    assertThat(duplicateJob, containsString("JOB_ALREADY_EXISTS"));

    final String prestop = stopJob(jobId, testHost());
    assertThat(prestop, containsString("JOB_NOT_DEPLOYED"));

    // Deploy job
    deployJob(jobId, testHost());

    // Stop job
    final String stop1 = stopJob(jobId, BOGUS_HOST);
    assertThat(stop1, containsString("HOST_NOT_FOUND"));
    final String stop2 = stopJob(BOGUS_JOB, testHost());
    assertThat(stop2, containsString("Unknown job"));
    final String stop3 = stopJob(jobId, testHost());
    assertThat(stop3, containsString(testHost() + ": done"));

    // Verify that undeploying the job from a nonexistent host fails
    assertThat(cli("undeploy", jobId.toString(), BOGUS_HOST), containsString("HOST_NOT_FOUND"));

    // Verify that undeploying a nonexistent job from the host fails
    assertThat(cli("undeploy", BOGUS_JOB.toString(), testHost()), containsString("Unknown job"));

    // Undeploy job
    undeployJob(jobId, testHost());
  }
}
