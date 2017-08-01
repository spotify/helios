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

import static com.google.common.base.CharMatcher.WHITESPACE;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Test;

public class ConfigFileJobCreationTest extends SystemTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    final String name = testJobName;
    final String version = "17";
    final String image = BUSYBOX;
    final Map<String, PortMapping> ports = ImmutableMap.of(
        "foo", PortMapping.of(4711),
        "bar", PortMapping.of(5000, externalPort));
    final Map<ServiceEndpoint, ServicePorts> registration = ImmutableMap.of(
        ServiceEndpoint.of("foo-service", "tcp"), ServicePorts.of("foo"),
        ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");

    final Map<String, Object> configuration = ImmutableMap.of("id", name + ":" + version,
        "image", image,
        "ports", ports,
        "registration", registration,
        "env", env);

    final Path file = temporaryFolder.newFile().toPath();
    Files.write(file, Json.asBytes(configuration));

    final String output = cli("create", "-q", "-f", file.toAbsolutePath().toString());
    final JobId jobId = JobId.parse(WHITESPACE.trimFrom(output));

    final Map<JobId, Job> jobs = client.jobs().get();
    final Job job = jobs.get(jobId);

    assertEquals(name, job.getId().getName());
    assertEquals(version, job.getId().getVersion());
    assertEquals(ports, job.getPorts());
    assertEquals(env, job.getEnv());
    assertEquals(registration, job.getRegistration());
  }

}
