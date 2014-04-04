/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConfigFileJobCreationTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    final String name = "test";
    final String version = "17";
    final String image = "busybox";
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort1));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "hm"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");

    final Map<String, Object> configuration = ImmutableMap.of("id", name + ":" + version,
                                                              "image", image,
                                                              "ports", ports,
                                                              "registration", registration,
                                                              "env", env);

    final Path file = Files.createTempFile("helios", ".json");
    Files.write(file, Json.asBytes(configuration));

    final String output = cli("job", "create", "-q", "-f", file.toAbsolutePath().toString());
    final JobId jobId = JobId.parse(StringUtils.strip(output));

    final Map<JobId, Job> jobs = client.jobs().get();
    final Job job = jobs.get(jobId);

    assertEquals(name, job.getId().getName());
    assertEquals(version, job.getId().getVersion());
    assertEquals(ports, job.getPorts());
    assertEquals(env, job.getEnv());
    assertEquals(registration, job.getRegistration());
  }

}
