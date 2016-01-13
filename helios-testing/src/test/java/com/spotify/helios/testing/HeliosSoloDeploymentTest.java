/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.testing;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HeliosSoloDeploymentTest {

  @Test
  public void heliosSoloDeploymentTest() {
    assertThat(testResult(HeliosSoloDeploymentTestImpl.class), isSuccessful());
  }

  public static class HeliosSoloDeploymentTestImpl {

    public static final String IMAGE_NAME = "onescience/alpine:latest";

    private static final Logger log = LoggerFactory.getLogger(HeliosSoloDeploymentTestImpl.class);

    // TODO(negz): We want one deployment per test run, not one per test class.
    @ClassRule
    public static final HeliosDeploymentResource DEPLOYMENT = new HeliosDeploymentResource(
        HeliosSoloDeployment.fromEnv().build());

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .jobPrefix("HeliosSoloDeploymentTest")
        .client(DEPLOYMENT.client())
        .build();

    @Test
    public void testDeployToSolo() throws Exception {
      temporaryJobs.job()
          // while ".*" is the default in the local testing profile, explicitly specify it here
          // to avoid any extraneous environment variables for HELIOS_HOST_FILTER that might be set
          // on the build agent executing this test from interfering with the behavior we want here.
          // Since we are deploying on a self-contained helios-solo container, any
          // HELIOS_HOST_FILTER value set for other tests will never match the agent hostname
          // inside helios-solo.
          .hostFilter(".*")
          .command(asList("sh", "-c", "nc -l -v -p 4711 -e true"))
          .image(IMAGE_NAME)
          .port("netcat", 4711)
          .registration("foobar", "tcp", "netcat")
          .deploy();

      final Map<JobId, Job> jobs = DEPLOYMENT.client().jobs().get(15, SECONDS);
      log.info("{} jobs deployed on helios-solo", jobs.size());
      for (Job job : jobs.values()) {
        log.info("job on helios-solo: {}", job);
      }

      assertEquals("wrong number of jobs running", 1, jobs.size());
      for (Job j : jobs.values()) {
        assertEquals("wrong job running", IMAGE_NAME, j.getImage());
      }
    }
  }

  @Test
  public void testDockerHostContainsLocalhost() throws Exception {
    final  DockerClient dockerClient = mock(DockerClient.class);

    // the anonymous classes to override a method are to workaround the docker-client "messages"
    // having no mutators, fun
    when(dockerClient.info()).thenReturn(new Info() {
      @Override
      public String operatingSystem() {
        return "foo";
      }
    });

    // mock the call to dockerClient.createContainer so we can test the arguments passed to it
    final ArgumentCaptor<ContainerConfig> containerConfig =
        ArgumentCaptor.forClass(ContainerConfig.class);

    final ContainerCreation creation = mock(ContainerCreation.class);
    final String containerId = "abc123";
    when(creation.id()).thenReturn(containerId);

    // we have to mock out several other calls to get the HeliosSoloDeployment ctor
    // to return non-exceptionally:

    when(dockerClient.createContainer(containerConfig.capture(), anyString())).thenReturn(creation);

    when(dockerClient.inspectContainer(containerId)).thenReturn(new ContainerInfo() {
      @Override
      public NetworkSettings networkSettings() {
        final PortBinding binding = PortBinding.of("192.168.1.1",  5801);
        final Map<String, List<PortBinding>> ports =
            ImmutableMap.<String, List<PortBinding>>of("5801/tcp", ImmutableList.of(binding));

        return NetworkSettings.builder()
            .gateway("a-gate-way")
            .ports(ports)
            .build();
      }
    });

    when(dockerClient.waitContainer(containerId)).thenReturn(new ContainerExit() {
      @Override
      public Integer statusCode() {
        return 0;
      }
    });

    // finally build the thing ...
    HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .build();

    // .. so we can test what was passed
    boolean foundSolo = false;
    for (ContainerConfig cc : containerConfig.getAllValues()) {
      if (cc.image().contains("helios-solo")) {
        assertThat(containerConfig.getValue().hostConfig().binds(),
                   hasItem("/var/run/docker.sock:/var/run/docker.sock"));
        foundSolo = true;
      }
    }
    assertTrue("Could not find helios-solo container creation", foundSolo);
  }
}
