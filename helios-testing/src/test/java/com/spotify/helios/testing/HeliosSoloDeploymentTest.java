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


import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeliosSoloDeploymentTest {

  private static final String CONTAINER_ID = "abc123";
  private static final String BUSYBOX = "busybox:latest";
  public static final List<String> IDLE_COMMAND = asList(
      "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");

  private DockerClient dockerClient;
  private ArgumentCaptor<ContainerConfig> containerConfig;
  private ContainerCreation creation;

  @Before
  public void setup() throws Exception {
    this.dockerClient = mock(DockerClient.class);

    // the anonymous classes to override a method are to workaround the docker-client "messages"
    // having no mutators, fun
    when(this.dockerClient.info()).thenReturn(new Info() {
      @Override
      public String operatingSystem() {
        return "foo";
      }
    });

    // mock the call to dockerClient.createContainer so we can test the arguments passed to it
    this.containerConfig = ArgumentCaptor.forClass(ContainerConfig.class);

    this.creation = mock(ContainerCreation.class);
    when(this.creation.id()).thenReturn(CONTAINER_ID);

    when(this.dockerClient.createContainer(
        this.containerConfig.capture(), anyString())).thenReturn(this.creation);

    // we have to mock out several other calls to get the HeliosSoloDeployment ctor
    // to return non-exceptionally. the anonymous classes to override a method are to workaround
    // the docker-client "messages" having no mutators, fun
    when(this.dockerClient.info()).thenReturn(new Info() {
      @Override
      public String operatingSystem() {
        return "foo";
      }
    });

    when(this.dockerClient.inspectContainer(CONTAINER_ID)).thenReturn(new ContainerInfo() {
      @Override
      public NetworkSettings networkSettings() {
        final PortBinding binding = PortBinding.of("192.168.1.1", 5801);
        final Map<String, List<PortBinding>> ports =
            ImmutableMap.<String, List<PortBinding>>of("5801/tcp", ImmutableList.of(binding));

        return NetworkSettings.builder()
            .gateway("a-gate-way")
            .ports(ports)
            .build();
      }
    });

    when(this.dockerClient.waitContainer(CONTAINER_ID)).thenReturn(new ContainerExit() {
      @Override
      public Integer statusCode() {
        return 0;
      }
    });
  }

  @Test
  public void testDockerHostContainsLocalhost() throws Exception {
    HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        // a custom dockerhost to trigger the localhost logic
        .dockerHost(DockerHost.from("tcp://localhost:2375", ""))
        .build();

    boolean foundSolo = false;
    for (ContainerConfig cc : containerConfig.getAllValues()) {
      if (cc.image().contains("helios-solo")) {
        assertThat(cc.hostConfig().binds(), hasItem("/var/run/docker.sock:/var/run/docker.sock"));
        foundSolo = true;
      }
    }
    assertTrue("Could not find helios-solo container creation", foundSolo);
  }

  @Test
  public void testConfig() throws Exception {

    final String image = "helios-test";
    final String ns = "namespace";
    final String env = "stuff";

    Config config = ConfigFactory.empty()
        .withValue("helios.solo.profile", ConfigValueFactory.fromAnyRef("test"))
        .withValue("helios.solo.profiles.test.image", ConfigValueFactory.fromAnyRef(image))
        .withValue("helios.solo.profiles.test.namespace", ConfigValueFactory.fromAnyRef(ns))
        .withValue("helios.solo.profiles.test.env.TEST", ConfigValueFactory.fromAnyRef(env));

    HeliosSoloDeployment.Builder builder = new HeliosSoloDeployment.Builder(null, config);
    builder.dockerClient(dockerClient).build();

    boolean foundSolo = false;
    for (ContainerConfig cc : containerConfig.getAllValues()) {
      if (cc.image().contains(image)) {
        foundSolo = true;
        assertThat(cc.env(), hasItem("TEST=" + env));
        assertThat(cc.env(), hasItem("HELIOS_NAME=" + ns + ".solo.local"));
      }
    }
    assertTrue("Could not find helios-solo container creation", foundSolo);

  }

  @Test
  public void testDoesNotPullPresentProbeImage() throws Exception {
    when(this.dockerClient.inspectImage(HeliosSoloDeployment.PROBE_IMAGE))
        .thenReturn(mock(ImageInfo.class));

    HeliosSoloDeployment.builder()
        .dockerClient(this.dockerClient)
        .build();

    verify(this.dockerClient, never()).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testDoesPullAbsentProbeImage() throws Exception {
    when(this.dockerClient.inspectImage(HeliosSoloDeployment.PROBE_IMAGE))
        .thenThrow(new ImageNotFoundException(HeliosSoloDeployment.PROBE_IMAGE));

    HeliosSoloDeployment.builder()
        .dockerClient(this.dockerClient)
        .build();

    verify(this.dockerClient).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testAfterCleansUpLeftoverJobs() throws Throwable {
    final HeliosSoloDeployment soloDeployment =
        (HeliosSoloDeployment) HeliosSoloDeployment.fromEnv().build();
    final HeliosDeploymentResource soloResource = new HeliosDeploymentResource(soloDeployment);
    // Since we're not using @Rule, we have to call these methods explicitly.
    // Ensure helios-solo is ready.
    soloResource.before();

    final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .client(soloResource.client())
        .build();

    temporaryJobs.before();

    final TemporaryJob job1 = temporaryJobs.job()
        .image(BUSYBOX)
        .command(IDLE_COMMAND)
        .deploy();
    final TemporaryJob job2 = temporaryJobs.job()
        .image(BUSYBOX)
        .command(IDLE_COMMAND)
        .deploy();

    final Map<String, TaskStatus> statuses1 = job1.statuses();
    final Map<String, TaskStatus> statuses2 = job2.statuses();
    final List<String> containerIds = ImmutableList.<String>builder()
        .addAll(taskStatusesToContainerIds(statuses1))
        .addAll(taskStatusesToContainerIds(statuses2))
        .build();

    assertThat(containerIds.size(), equalTo(2));

    // Run HeliosSoloDeployment's after() before TemporaryJobs.after() to test if it'll clean up
    // leftover jobs not cleaned up by TemporaryJobs.
    soloResource.after();

    final List<String> runningContainerIds = runningContainerIds(
        DefaultDockerClient.fromEnv().build());
    // We expect the containers associated with the two temp jobs above to not be running.
    for (final String containerId : containerIds) {
      assertFalse(runningContainerIds.contains(containerId));
    }
    // The solo container should also not be running.
    assertFalse(runningContainerIds.contains(soloDeployment.heliosContainerId()));
  }

  private List<String> taskStatusesToContainerIds(final Map<String, TaskStatus> map) {
    final List<TaskStatus> statuses = Lists.newArrayList(map.values());

    return Lists.transform(statuses, new Function<TaskStatus, String>() {
      @Override
      public String apply(final TaskStatus status) {
        return status.getContainerId();
      }
    });
  }

  private List<String> runningContainerIds(final DockerClient client)
      throws DockerException, InterruptedException {
    final List<Container> containers = client.listContainers();
    return Lists.transform(containers, new Function<Container, String>() {
          @Override
          public String apply(final Container container) {
            return container.id();
          }
        });
  }
}
