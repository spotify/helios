/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
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
import com.spotify.helios.common.descriptors.HostStatus.Status;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class HeliosSoloDeploymentTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
  private static final JobId JOB_ID1 = JOB1.getId();
  private static final JobId JOB_ID2 = JOB2.getId();
  private static final TaskStatus TASK_STATUS1 = TaskStatus.newBuilder()
      .setJob(JOB1)
      .setGoal(Goal.START)
      .setState(TaskStatus.State.RUNNING)
      .setContainerId(CONTAINER_ID)
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
    final Info info = mock(Info.class);
    when(info.operatingSystem()).thenReturn("foo");
    when(this.dockerClient.info()).thenReturn(info);

    // mock the call to dockerClient.createContainer so we can test the arguments passed to it
    this.containerConfig = ArgumentCaptor.forClass(ContainerConfig.class);

    final ContainerCreation creation = mock(ContainerCreation.class);
    when(creation.id()).thenReturn(CONTAINER_ID);

    when(this.dockerClient.createContainer(
        this.containerConfig.capture(), anyString())).thenReturn(creation);

    // we have to mock out several other calls to get the HeliosSoloDeployment ctor
    // to return non-exceptionally. the anonymous classes to override a method are to workaround
    // the docker-client "messages" having no mutators, fun
    when(this.dockerClient.info()).thenReturn(info);

    final PortBinding binding = PortBinding.of("192.168.1.1", 5801);
    final ImmutableMap<String, List<PortBinding>> ports =
        ImmutableMap.<String, List<PortBinding>>of("5801/tcp", ImmutableList.of(binding));
    final ContainerInfo containerInfo = mock(ContainerInfo.class);
    final NetworkSettings networkSettings = mock(NetworkSettings.class);
    when(networkSettings.gateway()).thenReturn("a-gate-way");
    when(networkSettings.ports()).thenReturn(ports);
    when(containerInfo.networkSettings()).thenReturn(networkSettings);
    when(this.dockerClient.inspectContainer(CONTAINER_ID)).thenReturn(containerInfo);

    when(this.dockerClient.waitContainer(CONTAINER_ID)).thenReturn(ContainerExit.create(0));
  }

  private HeliosSoloDeployment buildHeliosSoloDeployment() {
    return buildHeliosSoloDeployment(DockerHost.from("tcp://localhost:2375", ""));
  }

  private HeliosSoloDeployment buildHeliosSoloDeployment(HeliosSoloDeployment.Builder builder) {
    return buildHeliosSoloDeployment(builder, DockerHost.from("tcp://localhost:2375", ""));
  }

  private HeliosSoloDeployment buildHeliosSoloDeployment(DockerHost dockerHost) {
    return buildHeliosSoloDeployment(HeliosSoloDeployment.builder(), dockerHost);
  }

  private HeliosSoloDeployment buildHeliosSoloDeployment(final HeliosSoloDeployment.Builder builder,
                                                         final DockerHost dockerHost) {
    return builder.dockerClient(dockerClient)
        .dockerHost(dockerHost)
        .heliosClient(heliosClient)
        .build();
  }

  @Test
  public void testDockerHostContainsLocalhost() throws Exception {
    buildHeliosSoloDeployment();

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

    final HeliosSoloDeployment deployment = buildHeliosSoloDeployment(
        new HeliosSoloDeployment.Builder(null, config));
    assertEquals(ns + ".solo.local", deployment.agentName());

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
  public void testConfigHasGoogleContainerRegistryCredentials() throws Exception {
    // generate a file that we will pretend contains GCR credentials, in order to verify that
    // HeliosSoloDeployment sets up the expected environment variables and volume binds
    // when this config value exists (and is a real file)
    final File credentialsFile = temporaryFolder.newFile("fake-credentials");
    final String credentialsPath = credentialsFile.getPath();

    final String image = "helios-test";

    final Config config = ConfigFactory.empty()
        .withValue("helios.solo.profile", ConfigValueFactory.fromAnyRef("test"))
        .withValue("helios.solo.profiles.test.image", ConfigValueFactory.fromAnyRef(image))
        .withValue("helios.solo.profiles.test.google-container-registry.credentials",
            ConfigValueFactory.fromAnyRef(credentialsPath)
        );

    buildHeliosSoloDeployment(new HeliosSoloDeployment.Builder(null, config));

    ContainerConfig soloContainerConfig = null;
    for (final ContainerConfig cc : containerConfig.getAllValues()) {
      if (cc.image().contains(image)) {
        soloContainerConfig = cc;
      }
    }

    assertNotNull("Could not find helios-solo container creation", soloContainerConfig);

    assertThat(soloContainerConfig.env(),
        hasItem("HELIOS_AGENT_OPTS=--docker-gcp-account-credentials=" + credentialsPath));

    final String credentialsParentPath = credentialsFile.getParent();
    assertThat(soloContainerConfig.hostConfig().binds(),
        hasItem(credentialsParentPath + ":" + credentialsParentPath + ":ro"));
  }

  @Test
  public void testGoogleContainerRegistryCredentialsDoesntExist() throws Exception {
    final String image = "helios-test";

    final Config config = ConfigFactory.empty()
        .withValue("helios.solo.profile", ConfigValueFactory.fromAnyRef("test"))
        .withValue("helios.solo.profiles.test.image", ConfigValueFactory.fromAnyRef(image))
        .withValue("helios.solo.profiles.test.google-container-registry.credentials",
            ConfigValueFactory.fromAnyRef("/dev/null/foo/bar")
        );

    buildHeliosSoloDeployment(new HeliosSoloDeployment.Builder(null, config));

    ContainerConfig soloContainerConfig = null;
    for (final ContainerConfig cc : containerConfig.getAllValues()) {
      if (cc.image().contains(image)) {
        soloContainerConfig = cc;
      }
    }

    assertNotNull("Could not find helios-solo container creation", soloContainerConfig);

    assertThat(soloContainerConfig.env(), not(hasItem(startsWith("HELIOS_AGENT_OPTS="))));
    assertThat(soloContainerConfig.hostConfig().binds(), not(hasItem(startsWith("/dev/null"))));
  }


  @Test
  public void testDoesNotPullPresentProbeImage() throws Exception {
    when(this.dockerClient.inspectImage(HeliosSoloDeployment.PROBE_IMAGE))
        .thenReturn(mock(ImageInfo.class));

    buildHeliosSoloDeployment();

    verify(this.dockerClient, never()).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testDoesPullAbsentProbeImage() throws Exception {
    when(this.dockerClient.inspectImage(HeliosSoloDeployment.PROBE_IMAGE))
        .thenThrow(new ImageNotFoundException(HeliosSoloDeployment.PROBE_IMAGE));

    buildHeliosSoloDeployment();

    verify(this.dockerClient).pull(HeliosSoloDeployment.PROBE_IMAGE);
  }

  @Test
  public void testUndeployLeftoverJobs() throws Exception {
    final HeliosSoloDeployment solo = buildHeliosSoloDeployment();

    final ListenableFuture<List<String>> hostsFuture = Futures.<List<String>>immediateFuture(
        ImmutableList.of(HOST1, HOST2));
    when(heliosClient.listHosts()).thenReturn(hostsFuture);

    // These futures represent HostStatuses when the job is still deployed
    final ListenableFuture<HostStatus> statusFuture11 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(Status.UP)
            .setStatuses(ImmutableMap.of(JOB_ID1, TASK_STATUS1))
            .setJobs(ImmutableMap.of(JOB_ID1, Deployment.of(JOB_ID1, Goal.START)))
            .build());
    final ListenableFuture<HostStatus> statusFuture21 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(Status.UP)
            .setStatuses(ImmutableMap.of(JOB_ID2, TASK_STATUS2))
            .setJobs(ImmutableMap.of(JOB_ID2, Deployment.of(JOB_ID2, Goal.START)))
            .build());

    // These futures represent HostStatuses when the job is undeployed
    final ListenableFuture<HostStatus> statusFuture12 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(Status.UP)
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .setJobs(ImmutableMap.of(JOB_ID1, Deployment.of(JOB_ID1, Goal.START)))
            .build());
    final ListenableFuture<HostStatus> statusFuture22 = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(Status.UP)
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .setJobs(ImmutableMap.of(JOB_ID2, Deployment.of(JOB_ID2, Goal.START)))
            .build());
    //noinspection unchecked
    when(heliosClient.hostStatus(HOST1)).thenReturn(statusFuture11);
    //noinspection unchecked
    when(heliosClient.hostStatus(HOST2)).thenReturn(statusFuture21);

    final ListenableFuture<JobUndeployResponse> undeployFuture1 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST1, JOB_ID1));
    final ListenableFuture<JobUndeployResponse> undeployFuture2 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST2, JOB_ID2));

    // when undeploy is called, respond correctly & patch the mock to return
    // the undeployed HostStatus
    when(heliosClient.undeploy(JOB_ID1, HOST1)).thenAnswer(
        new Answer<ListenableFuture<JobUndeployResponse>>() {
          @Override
          public ListenableFuture<JobUndeployResponse> answer(final InvocationOnMock invocation)
              throws Throwable {
            when(heliosClient.hostStatus(HOST1)).thenReturn(statusFuture12);
            return undeployFuture1;
          }
        });
    when(heliosClient.undeploy(JOB_ID2, HOST2)).thenAnswer(
        new Answer<ListenableFuture<JobUndeployResponse>>() {
          @Override
          public ListenableFuture<JobUndeployResponse> answer(final InvocationOnMock invocation)
              throws Throwable {
            when(heliosClient.hostStatus(HOST1)).thenReturn(statusFuture22);
            return undeployFuture2;
          }
        });

    solo.undeployLeftoverJobs();

    verify(heliosClient).undeploy(JOB_ID1, HOST1);
    verify(heliosClient).undeploy(JOB_ID2, HOST2);
  }

  @Test
  public void testUndeployLeftoverJobs_noLeftoverJobs() throws Exception {
    final HeliosSoloDeployment solo = buildHeliosSoloDeployment();

    final ListenableFuture<Map<JobId, Job>> jobsFuture = Futures.immediateFuture(
        Collections.<JobId, Job>emptyMap());
    when(heliosClient.jobs()).thenReturn(jobsFuture);

    solo.undeployLeftoverJobs();

    // There should be no more calls to any HeliosClient methods.
    verify(heliosClient, never()).jobStatus(Matchers.any(JobId.class));
  }

  @Test
  public void testLogService() throws Exception {
    final InMemoryLogStreamFollower logStreamProvider = InMemoryLogStreamFollower.create();
    final HeliosSoloLogService logService = new HeliosSoloLogService(heliosClient,
        dockerClient,
        logStreamProvider);

    final ListenableFuture<List<String>> hostsFuture = Futures.<List<String>>immediateFuture(
        ImmutableList.of(HOST1));
    when(heliosClient.listHosts()).thenReturn(hostsFuture);

    final ListenableFuture<HostStatus> statusFuture = Futures.immediateFuture(
        HostStatus.newBuilder()
            .setStatus(Status.UP)
            .setStatuses(ImmutableMap.of(JOB_ID1, TASK_STATUS1))
            .setJobs(ImmutableMap.of(JOB_ID1, Deployment.of(JOB_ID1, Goal.START)))
            .build());
    when(heliosClient.hostStatus(HOST1)).thenReturn(statusFuture);

    when(dockerClient.logs(anyString(), Matchers.<DockerClient.LogsParam>anyVararg()))
        .thenReturn(mock(LogStream.class));
    logService.runOneIteration();

    verify(dockerClient, timeout(5000)).logs(eq(CONTAINER_ID),
        Matchers.<DockerClient.LogsParam>anyVararg());
  }
}
