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

import com.google.common.base.Charsets;
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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CliJobCreationTest extends SystemTestBase {

  private final Integer externalPort = temporaryPorts().localPort("external");
  private final String testJobNameAndVersion = testJobName + ":" + testJobVersion;

  @Before
  public void initialize() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
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
        ServiceEndpoint.of("foo-service", "hm"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");
    final Map<String, String> volumes = Maps.newHashMap();
    volumes.put("/etc/spotify/secret-keys.yaml:ro", "/etc/spotify/secret-keys.yaml");

    // Create a new job, serialize it to JSON
    final Job.Builder builder = Job.newBuilder()
        .setCommand(Lists.newArrayList("server", "foo-service.yaml"))
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .setVolumes(volumes);
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
      Job.Builder actualJobBuilder = actualJob.toBuilder();
      builder.setName(testJobName).setVersion(testJobVersion).setImage(BUSYBOX);
      assertEquals(builder.build(), actualJobBuilder.build());
    }
  }

  @Test
  public void testSuccessJsonOutput() throws Exception {
    // Creating a valid job should return JSON with status OK
    String output = cli("create", "--json", testJobNameAndVersion, BUSYBOX);
    CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.OK, createJobResponse.getStatus());
    assertEquals(new ArrayList<String>(), createJobResponse.getErrors());
    assertTrue(createJobResponse.getId().startsWith(testJobNameAndVersion));
  }

  @Test
  public void testInvalidJobJsonOutput() throws Exception {
    // Trying to create a job with an invalid image name should return JSON with
    // INVALID_JOB_DEFINITION
    String output = cli("create", "--json", testJobNameAndVersion, "DOES_NOT_LIKE_AT_ALL-CAPITALS");
    CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.INVALID_JOB_DEFINITION, createJobResponse.getStatus());
    assertTrue(createJobResponse.getId().startsWith(testJobNameAndVersion));
  }

  @Test
  public void testTemplateUnknownJobJsonOutput() throws Exception {
    // Trying to create a job with a non-existant job as a template should return JSON with
    // UNKNOWN_JOB
    String output =
        cli("create", "--json", "--template", "non-existant-job", testJobNameAndVersion, BUSYBOX);
    CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.UNKNOWN_JOB, createJobResponse.getStatus());
  }

  @Test
  public void testTemplateAmbiguousJobJsonOutput() throws Exception {
    // Create two jobs
    cli("create", testJobNameAndVersion, BUSYBOX);
    cli("create", testJobNameAndVersion + "1", BUSYBOX);

    // Trying to create a job with an ambiguous template reference should return JSON with
    // AMBIGUOUS_JOB_REFERENCE
    String output = cli("create", "--json", "--template", testJobNameAndVersion,
                        testJobNameAndVersion, BUSYBOX);
    CreateJobResponse createJobResponse = Json.read(output, CreateJobResponse.class);
    assertEquals(CreateJobResponse.Status.AMBIGUOUS_JOB_REFERENCE, createJobResponse.getStatus());
  }
}
