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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeliosSoloDeploymentTest {

  private static final String CONTAINER_ID = "abc123";
  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final Job JOB1 = Job.newBuilder()
      .setCommand(ImmutableList.of("BOGUS1"))
      .setImage("IMAGE")
      .setName("NAME")
      .setVersion("VERSION")
      .build();
  private static final Job JOB2 = Job.newBuilder()
      .setCommand(ImmutableList.of("BOGUS2"))
      .setImage("IMAGE")
      .setName("NAME")
      .setVersion("VERSION")
      .build();
  private static final TaskStatus TASK_STATUS1 = TaskStatus.newBuilder()
      .setJob(JOB1)
      .setGoal(Goal.START)
      .setState(TaskStatus.State.RUNNING)
      .build();
  private static final TaskStatus TASK_STATUS2 = TaskStatus.newBuilder()
      .setJob(JOB2)
      .setGoal(Goal.START)
      .setState(TaskStatus.State.RUNNING)
      .build();

  private DockerClient dockerClient;
  private HeliosClient heliosClient;
  private ArgumentCaptor<ContainerConfig> containerConfig;

  @Before
  public void setup() throws Exception {
    this.dockerClient = mock(DockerClient.class);
    this.heliosClient = mock(HeliosClient.class);

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

    final ContainerCreation creation = mock(ContainerCreation.class);
    when(creation.id()).thenReturn(CONTAINER_ID);

    when(this.dockerClient.createContainer(
        this.containerConfig.capture(), anyString())).thenReturn(creation);

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
  public void testUndeployLeftoverJobs() throws Exception {
    final HeliosSoloDeployment solo = (HeliosSoloDeployment) HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .heliosClient(heliosClient)
        .build();

    final ListenableFuture<Map<JobId, Job>> jobsFuture = Futures.<Map<JobId, Job>>immediateFuture(
        ImmutableMap.of(JOB1.getId(), JOB1, JOB2.getId(), JOB2));
    when(heliosClient.jobs()).thenReturn(jobsFuture);

    final ListenableFuture<JobStatus> statusFuture1 = Futures.immediateFuture(
        JobStatus.newBuilder().setTaskStatuses(ImmutableMap.of(HOST1, TASK_STATUS1)).build());
    final ListenableFuture<JobStatus> statusFuture2 = Futures.immediateFuture(
        JobStatus.newBuilder().setTaskStatuses(ImmutableMap.of(HOST2, TASK_STATUS2)).build());
    when(heliosClient.jobStatus(JOB1.getId())).thenReturn(statusFuture1);
    when(heliosClient.jobStatus(JOB2.getId())).thenReturn(statusFuture2);

    final ListenableFuture<JobUndeployResponse> undeployFuture1 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST1, JOB1.getId()));
    final ListenableFuture<JobUndeployResponse> undeployFuture2 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST2, JOB2.getId()));
    when(heliosClient.undeploy(JOB1.getId(), HOST1)).thenReturn(undeployFuture1);
    when(heliosClient.undeploy(JOB2.getId(), HOST2)).thenReturn(undeployFuture2);

    final ListenableFuture<HostStatus> hostStatusFuture1 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(HostStatus.Status.UP)
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .setJobs(ImmutableMap.of(JOB1.getId(), Deployment.of(JOB1.getId(), Goal.START)))
            .build());
    final ListenableFuture<HostStatus> hostStatusFuture2 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(HostStatus.Status.UP)
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .setJobs(ImmutableMap.of(JOB2.getId(), Deployment.of(JOB2.getId(), Goal.START)))
            .build());
    when(heliosClient.hostStatus(HOST1)).thenReturn(hostStatusFuture1);
    when(heliosClient.hostStatus(HOST2)).thenReturn(hostStatusFuture2);

    solo.undeployLeftoverJobs();

    verify(heliosClient).undeploy(JOB1.getId(), HOST1);
    verify(heliosClient).undeploy(JOB2.getId(), HOST2);
  }

  @Test
  public void testUndeployLeftoverJobs_noLeftoverJobs() throws Exception {
    final HeliosSoloDeployment solo = (HeliosSoloDeployment) HeliosSoloDeployment.builder()
        .dockerClient(dockerClient)
        .heliosClient(heliosClient)
        .build();

    final ListenableFuture<Map<JobId, Job>> jobsFuture = Futures.immediateFuture(
        Collections.<JobId, Job>emptyMap());
    when(heliosClient.jobs()).thenReturn(jobsFuture);

    solo.undeployLeftoverJobs();

    // There should be no more calls to any HeliosClient methods.
    verify(heliosClient, never()).jobStatus(Matchers.any(JobId.class));
  }
}
