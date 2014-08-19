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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class CliJobCreationTest extends SystemTestBase {

  private static final JobId BOGUS_JOB = new JobId("bogus", "job", Strings.repeat("0", 40));
  private static final String BOGUS_HOST = "BOGUS_HOST";


  private static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};

  private final Integer externalPort = temporaryPorts().localPort("external");

  @Test
  public void testMergeFileAndCliArgs() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });

    final String image = "busybox";
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
      cli("create", "--file", absolutePath, testJobName + ":" + testJobVersion, image);

      // Inspect the job.
      final String actualJobConfigJson = cli("inspect", testJobName + ":" + testJobVersion,
                                             "--json");

      // Compare to make sure the created job has the expected configuration,
      // i.e. the configuration resulting from a merge of the JSON file and CLI args.
      final Job actualJob = Json.read(actualJobConfigJson, Job.class);
      Job.Builder actualJobBuilder = actualJob.toBuilder();
      builder.setName(testJobName).setVersion(testJobVersion).setImage(image);
      assertEquals(builder.build(), actualJobBuilder.build());
    }
  }
}
