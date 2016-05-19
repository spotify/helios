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
import com.google.common.net.HostAndPort;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.client.HeliosClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.spotify.helios.testing.HeliosSoloDeployment.HELIOS_MASTER_PORT;
import static com.spotify.helios.testing.HeliosSoloDeployment.HELIOS_SOLO_WATCHDOG_PORT;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeliosSoloDeploymentTest {

  private static final String PROBE_CONTAINER_ID = "probe123";
  private static final String SOLO_CONTAINER_ID = "solo123";

  private DockerClient dockerClient;
  private ArgumentCaptor<ContainerConfig> containerConfig;
  private SoloMasterProber soloMasterProber;
  private SoloAgentProber soloAgentProber;
  private SoloWatchdogConnector soloWatchdogConnector;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    dockerClient = mock(DockerClient.class);
    soloMasterProber = mock(SoloMasterProber.class);
    soloAgentProber = mock(SoloAgentProber.class);
    soloWatchdogConnector = mock(SoloWatchdogConnector.class);

    when(soloMasterProber.check(any(HostAndPort.class))).thenReturn(true);
    when(soloAgentProber.check(any(HeliosClient.class))).thenReturn(true);

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

    final ContainerCreation probeCreation = mock(ContainerCreation.class);
    when(probeCreation.id()).thenReturn(PROBE_CONTAINER_ID);
    final ContainerCreation soloCreation = mock(ContainerCreation.class);
    when(soloCreation.id()).thenReturn(SOLO_CONTAINER_ID);

    when(this.dockerClient.createContainer(
        this.containerConfig.capture(), anyString())).thenReturn(probeCreation, soloCreation);

    // we have to mock out several other calls to get the HeliosSoloDeployment ctor
    // to return non-exceptionally. the anonymous classes to override a method are to workaround
    // the docker-client "messages" having no mutators, fun
    when(this.dockerClient.info()).thenReturn(new Info() {
      @Override
      public String operatingSystem() {
        return "foo";
      }
    });

    when(this.dockerClient.inspectContainer(PROBE_CONTAINER_ID)).thenReturn(new ContainerInfo() {
      @Override
      public NetworkSettings networkSettings() {
        return NetworkSettings.builder()
            .gateway("a-gate-way")
            .build();
      }
    });

    when(this.dockerClient.inspectContainer(SOLO_CONTAINER_ID)).thenReturn(new ContainerInfo() {
      @Override
      public NetworkSettings networkSettings() {
        final PortBinding masterPortBinding = PortBinding.of("192.168.1.1", 5801);
        final PortBinding watchdogPortBinding =
            PortBinding.of("192.168.1.1", HELIOS_SOLO_WATCHDOG_PORT);
        final Map<String, List<PortBinding>> ports = ImmutableMap.<String, List<PortBinding>>of(
            HELIOS_MASTER_PORT + "/tcp", ImmutableList.of(masterPortBinding),
            HELIOS_SOLO_WATCHDOG_PORT + "/tcp", ImmutableList.of(watchdogPortBinding)
        );

        return NetworkSettings.builder()
            .gateway("a-gate-way")
            .ports(ports)
            .build();
      }
    });

    when(this.dockerClient.waitContainer(PROBE_CONTAINER_ID)).thenReturn(new ContainerExit() {
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
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .build();

    boolean foundSolo = false;
    for (final ContainerConfig cc : containerConfig.getAllValues()) {
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

    final Config config = ConfigFactory.empty()
        .withValue("helios.solo.profile", ConfigValueFactory.fromAnyRef("test"))
        .withValue("helios.solo.profiles.test.image", ConfigValueFactory.fromAnyRef(image))
        .withValue("helios.solo.profiles.test.namespace", ConfigValueFactory.fromAnyRef(ns))
        .withValue("helios.solo.profiles.test.env.TEST", ConfigValueFactory.fromAnyRef(env));

    HeliosSoloDeployment.builderWithProfileAndConfig(null, config)
        .dockerClient(dockerClient)
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .build();

    boolean foundSolo = false;
    for (final ContainerConfig cc : containerConfig.getAllValues()) {
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
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .build();

    verify(this.dockerClient, never()).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testDoesPullAbsentProbeImage() throws Exception {
    when(this.dockerClient.inspectImage(HeliosSoloDeployment.PROBE_IMAGE))
        .thenThrow(new ImageNotFoundException(HeliosSoloDeployment.PROBE_IMAGE));

    HeliosSoloDeployment.builder()
        .dockerClient(this.dockerClient)
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .build();

    verify(this.dockerClient).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testConnectToWatchdogWithTimeoutSuccess() throws Exception {
    final SoloWatchdogConnector connector = mock(SoloWatchdogConnector.class);
    HeliosSoloDeployment.connectToWatchdogWithTimeout("host", 123, 10, TimeUnit.SECONDS, connector);

    // Test a successful socket connection within the time limit doesn't result in RuntimeException
    doThrow(new IOException()).doNothing().when(connector).connect(
        any(Socket.class), anyString(), anyInt());
    HeliosSoloDeployment.connectToWatchdogWithTimeout("host", 123, 2, TimeUnit.SECONDS, connector);
  }

  @Test
  public void testConnectToWatchdogWithTimeoutFailure() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectCause(isA(TimeoutException.class));

    final SoloWatchdogConnector connector = mock(SoloWatchdogConnector.class);
    doThrow(new IOException()).when(connector).connect(any(Socket.class), anyString(), anyInt());
    HeliosSoloDeployment.connectToWatchdogWithTimeout("host", 123, 2, TimeUnit.SECONDS, connector);
  }

  @Test
  public void testCloseSoloExitsInTime() throws Exception {
    final HeliosSoloDeployment solo = HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .build();

    solo.close();

    verify(dockerClient).waitContainer(SOLO_CONTAINER_ID);
    verify(dockerClient, never()).killContainer(SOLO_CONTAINER_ID);
    verify(dockerClient).removeContainer(SOLO_CONTAINER_ID);
    verify(dockerClient).close();
  }

  @Test
  public void testCloseSoloExitsTooLate() throws Exception {
    final HeliosSoloDeployment solo = HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .soloExitTimeoutSeconds(1)
        .build();

    when(dockerClient.waitContainer(anyString())).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1500);
        return null;
      }
    });

    solo.close();

    verify(dockerClient).waitContainer(SOLO_CONTAINER_ID);
    verify(dockerClient).killContainer(SOLO_CONTAINER_ID);
    verify(dockerClient).removeContainer(SOLO_CONTAINER_ID);
    verify(dockerClient).close();
  }

  @Test
  public void testCloseSoloDoNotRemoveContainer() throws Exception {
    final HeliosSoloDeployment solo = HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .soloMasterProber(soloMasterProber)
        .soloAgentProber(soloAgentProber)
        .soloWatchdogConnector(soloWatchdogConnector)
        .removeHeliosSoloOnExit(false)
        .build();

    solo.close();

    verify(dockerClient, never()).removeContainer(SOLO_CONTAINER_ID);
  }
}
