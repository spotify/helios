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
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HeliosSoloDeploymentTest {

  private static final String CONTAINER_ID = "abc123";

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
}
