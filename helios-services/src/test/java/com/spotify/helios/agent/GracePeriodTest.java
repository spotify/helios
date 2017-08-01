/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.agent;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.CREATING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPING;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.NopServiceRegistrationHandle;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class GracePeriodTest {

  final ExecutorService executor = Executors.newCachedThreadPool();
  static final TemporaryPorts TEMPORARY_PORTS = TemporaryPorts.create();

  static final String NAMESPACE = "helios-deadbeef";
  static final String REPOSITORY = "spotify";
  static final String TAG = "17";
  static final String IMAGE = REPOSITORY + ":" + TAG;
  static final String NAME = "foobar";
  static final List<String> COMMAND = asList("foo", "bar");
  static final Integer EXTERNAL_PORT = TEMPORARY_PORTS.localPort("external");
  static final Map<String, PortMapping> PORTS = ImmutableMap.of(
      "bar", PortMapping.of(5000, EXTERNAL_PORT)
  );
  static final Map<ServiceEndpoint, ServicePorts> REGISTRATION = ImmutableMap.of(
      ServiceEndpoint.of("foo-service", "tcp"), ServicePorts.of("foo"),
      ServiceEndpoint.of("bar-service", "http"), ServicePorts.of("bar"));
  static final String VERSION = "4711";
  static final Integer GRACE_PERIOD = 60;
  static final long GRACE_PERIOD_MILLIS =
      TimeUnit.MILLISECONDS.convert(GRACE_PERIOD, TimeUnit.SECONDS);
  static final Job JOB = Job.newBuilder()
      .setName(NAME)
      .setCommand(COMMAND)
      .setImage(IMAGE)
      .setPorts(PORTS)
      .setRegistration(REGISTRATION)
      .setVersion(VERSION)
      .setGracePeriod(GRACE_PERIOD)
      .build();
  static final Map<String, String> ENV = ImmutableMap.of("foo", "17", "bar", "4711");
  static final Set<String> EXPECTED_CONTAINER_ENV = ImmutableSet.of("foo=17", "bar=4711");

  public final ContainerInfo runningResponse = mock(ContainerInfo.class);

  public final ContainerInfo stoppedResponse = mock(ContainerInfo.class);

  @Mock public AgentModel model;
  @Mock public DockerClient docker;
  @Mock public RestartPolicy retryPolicy;
  @Mock public ServiceRegistrar registrar;
  @Mock public Sleeper sleeper;

  @Captor public ArgumentCaptor<ContainerConfig> containerConfigCaptor;
  @Captor public ArgumentCaptor<String> containerNameCaptor;
  @Captor public ArgumentCaptor<TaskStatus> taskStatusCaptor;

  Supervisor sut;

  @Before
  public void setup() throws Exception {
    final ContainerState runningState = Mockito.mock(ContainerState.class);
    when(runningState.running()).thenReturn(true);
    when(runningResponse.state()).thenReturn(runningState);
    when(runningResponse.networkSettings()).thenReturn(mock(NetworkSettings.class));

    final ContainerState stoppedState = Mockito.mock(ContainerState.class);
    when(stoppedState.running()).thenReturn(false);
    when(stoppedResponse.state()).thenReturn(stoppedState);

    when(retryPolicy.delay(any(ThrottleState.class))).thenReturn(10L);
    when(registrar.register(any(ServiceRegistration.class)))
        .thenReturn(new NopServiceRegistrationHandle());

    final TaskConfig config = TaskConfig.builder()
        .namespace(NAMESPACE)
        .host("AGENT_NAME")
        .job(JOB)
        .envVars(ENV)
        .defaultRegistrationDomain("domain")
        .build();

    final TaskStatus.Builder taskStatus = TaskStatus.newBuilder()
        .setJob(JOB)
        .setEnv(ENV)
        .setPorts(PORTS);

    final StatusUpdater statusUpdater = new DefaultStatusUpdater(model, taskStatus);
    final TaskMonitor monitor = new TaskMonitor(
        JOB.getId(), FlapController.create(), statusUpdater);

    final TaskRunnerFactory runnerFactory = TaskRunnerFactory.builder()
        .registrar(registrar)
        .config(config)
        .dockerClient(docker)
        .listener(monitor)
        .build();

    sut = Supervisor.newBuilder()
        .setJob(JOB)
        .setStatusUpdater(statusUpdater)
        .setDockerClient(docker)
        .setRestartPolicy(retryPolicy)
        .setRunnerFactory(runnerFactory)
        .setMetrics(new NoopSupervisorMetrics())
        .setMonitor(monitor)
        .setSleeper(sleeper)
        .build();

    final ConcurrentMap<JobId, TaskStatus> statusMap = Maps.newConcurrentMap();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) {
        final Object[] arguments = invocationOnMock.getArguments();
        final JobId jobId = (JobId) arguments[0];
        final TaskStatus status = (TaskStatus) arguments[1];
        statusMap.put(jobId, status);
        return null;
      }
    }).when(model).setTaskStatus(eq(JOB.getId()), taskStatusCaptor.capture());
    when(model.getTaskStatus(eq(JOB.getId()))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
        final JobId jobId = (JobId) invocationOnMock.getArguments()[0];
        return statusMap.get(jobId);
      }
    });
  }

  @After
  public void teardown() throws Exception {
    if (sut != null) {
      sut.close();
      sut.join();
    }
  }

  @Test
  public void verifySupervisorStartsAndStopsDockerContainer() throws Exception {
    final String containerId = "deadbeef";

    final ContainerCreation createResponse = ContainerCreation.builder().id(containerId).build();

    final SettableFuture<ContainerCreation> createFuture = SettableFuture.create();
    when(docker.createContainer(any(ContainerConfig.class),
        any(String.class))).thenAnswer(futureAnswer(createFuture));

    final SettableFuture<Void> startFuture = SettableFuture.create();
    doAnswer(futureAnswer(startFuture))
        .when(docker).startContainer(eq(containerId));

    final ImageInfo imageInfo = mock(ImageInfo.class);
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    final SettableFuture<ContainerExit> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenAnswer(futureAnswer(waitFuture));

    // Start the job
    sut.setGoal(START);

    // Verify that the pulling state is signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(PULLING_IMAGE)
            .setPorts(PORTS)
            .setContainerId(null)
            .setEnv(ENV)
            .build())
    );

    // Verify that the container is created
    verify(docker, timeout(30000)).createContainer(containerConfigCaptor.capture(),
        containerNameCaptor.capture());
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(CREATING)
            .setPorts(PORTS)
            .setContainerId(null)
            .setEnv(ENV)
            .build())
    );
    createFuture.set(createResponse);
    final ContainerConfig containerConfig = containerConfigCaptor.getValue();
    assertEquals(IMAGE, containerConfig.image());
    assertEquals(EXPECTED_CONTAINER_ENV, ImmutableSet.copyOf(containerConfig.env()));
    final String containerName = containerNameCaptor.getValue();

    assertEquals(JOB.getId().toShortString(), shortJobIdFromContainerName(containerName));

    // Verify that the container is started
    verify(docker, timeout(30000)).startContainer(eq(containerId));
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(STARTING)
            .setPorts(PORTS)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );
    when(docker.inspectContainer(eq(containerId))).thenReturn(runningResponse);
    startFuture.set(null);

    verify(docker, timeout(30000)).waitContainer(containerId);
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(RUNNING)
            .setPorts(PORTS)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );

    // Stop the job
    final SettableFuture<Void> killFuture = SettableFuture.create();
    doAnswer(futureAnswer(killFuture)).when(docker).killContainer(eq(containerId));
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO (dano): Make Supervisor.stop() asynchronous
        sut.setGoal(STOP);
        return null;
      }
    });

    // Stop the container
    verify(docker, timeout(30000)).killContainer(eq(containerId));

    // Verify that Sleeper has been called and that datetime has increased by
    // GRACE_PERIOD number of milliseconds
    verify(sleeper).sleep(GRACE_PERIOD_MILLIS);

    // Change docker container state to stopped when it's killed
    when(docker.inspectContainer(eq(containerId))).thenReturn(stoppedResponse);
    killFuture.set(null);

    // Verify that the stopping state is signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(STOP)
            .setState(STOPPING)
            .setPorts(PORTS)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );

    // Verify that the stopped state is signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(STOP)
            .setState(STOPPED)
            .setPorts(PORTS)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );
  }

  private String shortJobIdFromContainerName(final String containerName) {
    assertThat(containerName, startsWith(NAMESPACE + "-"));
    final String name = containerName.substring(NAMESPACE.length() + 1);
    final int lastUnderscore = name.lastIndexOf('_');
    return name.substring(0, lastUnderscore).replace('_', ':');
  }

  private Answer<?> futureAnswer(final SettableFuture<?> future) {
    return new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        return future.get();
      }
    };
  }

}
