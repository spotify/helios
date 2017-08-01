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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.protocol.CreateJobResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.Before;
import org.junit.Test;

public class CliJobCreationTest extends SystemTestBase {

  private final Integer externalPort = temporaryPorts().localPort("external");

  @Before
  public void initialize() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });
  }

  @Test
  public void testMergeFileAndCliArgs() throws Exception {
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "tcp"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final String registrationDomain = "my-domain";
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");
    final Map<String, String> volumes = Maps.newHashMap();
    volumes.put("/etc/spotify/secret-keys.yaml:ro", "/etc/spotify/secret-keys.yaml");

    // Create a new job, serialize it to JSON
    final Job.Builder builder = Job.newBuilder()
        .setCommand(Lists.newArrayList("server", "foo-service.yaml"))
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .setRegistrationDomain(registrationDomain)
        .setVolumes(volumes)
        .setCreatingUser(TEST_USER)
        .setToken("foo-token")
        .setNetworkMode("host")
        .setSecurityOpt(ImmutableList.of("label:user:dxia", "apparmor:foo"));
    final Job job = builder.build();
    final String jobConfigJsonString = job.toJsonString();

    // Create temporary job config file
    final File file = temporaryFolder.newFile();
    final String absolutePath = file.getAbsolutePath();

    // Write JSON config to temp file
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(jobConfigJsonString).array());

      // Create job and specify the temp file.
      cli("create", "--file", absolutePath, testJobNameAndVersion, BUSYBOX);

      // Inspect the job.
      final String actualJobConfigJson = cli("inspect", testJobNameAndVersion, "--json");

      // Compare to make sure the created job has the expected configuration,
      // i.e. the configuration resulting from a merge of the JSON file and CLI args.
      final Job actualJob = Json.read(actualJobConfigJson, Job.class);
      final Job.Builder actualJobBuilder = actualJob.toBuilder();
      builder.setName(testJobName).setVersion(testJobVersion).setImage(BUSYBOX);
      assertJobEquals(builder.build(), actualJobBuilder.build());
    }
  }

  @Test
  public void testSuccessJsonOutput() throws Exception {
    // Creating a valid job should return JSON with status OK
    final String output = cli("create", "--json", testJobNameAndVersion, BUSYBOX);
    final CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.OK, createJobResponse.getStatus());
    assertEquals(new ArrayList<String>(), createJobResponse.getErrors());
    assertTrue(createJobResponse.getId().startsWith(testJobNameAndVersion));

    // Check the master has set the created field
    final String output2 = cli("inspect", testJobNameAndVersion, "--json");
    final Job job = Json.read(output2, Job.class);
    assertNotNull(job.getCreated());
  }

  @Test
  public void testInvalidJobJsonOutput() throws Exception {
    // Trying to create a job with an invalid image name should return JSON with
    // INVALID_JOB_DEFINITION
    final String output = cli("create", "--json", testJobNameAndVersion,
        "DOES_NOT_LIKE_AT_ALL-CAPITALS");
    final CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.INVALID_JOB_DEFINITION, createJobResponse.getStatus());
    assertTrue(createJobResponse.getId().startsWith(testJobNameAndVersion));
  }

  @Test
  public void testTemplateUnknownJobJsonOutput() throws Exception {
    // Trying to create a job with a non-existant job as a template should return JSON with
    // UNKNOWN_JOB
    final String output =
        cli("create", "--json", "--template", "non-existant-job", testJobNameAndVersion, BUSYBOX);
    final CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.UNKNOWN_JOB, createJobResponse.getStatus());
  }

  @Test
  public void testTemplateAmbiguousJobJsonOutput() throws Exception {
    // Create two jobs
    cli("create", testJobNameAndVersion, BUSYBOX);
    cli("create", testJobNameAndVersion + "1", BUSYBOX);

    // Trying to create a job with an ambiguous template reference should return JSON with
    // AMBIGUOUS_JOB_REFERENCE
    final String output = cli("create", "--json", "--template", testJobNameAndVersion,
        testJobNameAndVersion, BUSYBOX);
    final CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.AMBIGUOUS_JOB_REFERENCE, createJobResponse.getStatus());
  }

  @Test
  public void testMergeFileAndCliArgsEnvPrecedence() throws Exception {
    final String redundantEnvKey = "BAD";
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "tcp"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of(redundantEnvKey, "f00d");
    final Map<String, String> volumes = Maps.newHashMap();
    volumes.put("/etc/spotify/secret-keys.yaml:ro", "/etc/spotify/secret-keys.yaml");

    // Create a new job, serialize it to JSON
    final Job.Builder builder = Job.newBuilder()
        .setCommand(Lists.newArrayList("server", "foo-service.yaml"))
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .setVolumes(volumes)
        .setCreatingUser(TEST_USER);
    final Job job = builder.build();
    final String jobConfigJsonString = job.toJsonString();

    // Create temporary job config file
    final File file = temporaryFolder.newFile();
    final String absolutePath = file.getAbsolutePath();

    // Write JSON config to temp file
    try (final FileOutputStream outFile = new FileOutputStream(file)) {
      outFile.write(Charsets.UTF_8.encode(jobConfigJsonString).array());

      // Create job and specify the temp file.
      cli("create", "--file", absolutePath, testJobNameAndVersion, BUSYBOX, "--env",
          redundantEnvKey + "=FOOD");

      // Inspect the job.
      final String actualJobConfigJson = cli("inspect", testJobNameAndVersion, "--json");

      // Compare to make sure the created job has the expected configuration,
      // i.e. the configuration resulting from a merge of the JSON file and CLI args.
      final Job actualJob = Json.read(actualJobConfigJson, Job.class);
      final Job.Builder actualJobBuilder = actualJob.toBuilder();
      builder.setName(testJobName).setVersion(testJobVersion).setImage(BUSYBOX)
          .setEnv(ImmutableMap.of(redundantEnvKey, "FOOD"));
      assertJobEquals(builder.build(), actualJobBuilder.build());
    }
  }
}
