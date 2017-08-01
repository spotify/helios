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
import static com.spotify.helios.common.descriptors.TaskStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.PULLING_IMAGE;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STARTING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPING;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerState;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SupervisorTest {

  private final ExecutorService executor = Executors.newCachedThreadPool();

  private static final String NAMESPACE = "helios-deadbeef";
  private static final String REPOSITORY = "spotify";
  private static final String TAG = "17";
  private static final String IMAGE = REPOSITORY + ":" + TAG;
  private static final String NAME = "foobar";
  private static final List<String> COMMAND = asList("foo", "bar");
  private static final String VERSION = "4711";
  private static final Job JOB = Job.newBuilder()
      .setName(NAME)
      .setCommand(COMMAND)
      .setImage(IMAGE)
      .setVersion(VERSION)
      .build();
  private static final Map<String, PortMapping> PORTS = Collections.emptyMap();
  private static final Map<String, String> ENV = ImmutableMap.of("foo", "17",
      "bar", "4711");
  private static final Set<String> EXPECTED_CONTAINER_ENV = ImmutableSet.of("foo=17", "bar=4711");

  private final ContainerInfo runningResponse = mock(ContainerInfo.class);

  private final ContainerInfo stoppedResponse = mock(ContainerInfo.class);

  @Mock public AgentModel model;
  @Mock public DockerClient docker;
  @Mock public RestartPolicy retryPolicy;
  @Mock public ServiceRegistrar registrar;
  @Mock public Sleeper sleeper;

  @Captor public ArgumentCaptor<ContainerConfig> containerConfigCaptor;
  @Captor public ArgumentCaptor<String> containerNameCaptor;
  @Captor public ArgumentCaptor<TaskStatus> taskStatusCaptor;

  private Supervisor sut;

  @Before
  public void setup() throws Exception {
    final ContainerState runningState = mock(ContainerState.class);
    when(runningState.running()).thenReturn(true);
    when(runningResponse.state()).thenReturn(runningState);
    when(runningResponse.networkSettings()).thenReturn(mock(NetworkSettings.class));

    final ContainerState stoppedState = mock(ContainerState.class);
    when(stoppedState.running()).thenReturn(false);
    when(stoppedResponse.state()).thenReturn(stoppedState);

    when(retryPolicy.delay(any(ThrottleState.class))).thenReturn(10L);

    sut = createSupervisor(JOB);
    mockTaskStatus(JOB.getId());
  }

  @After
  public void teardown() throws Exception {
    if (sut != null) {
      sut.close();
      sut.join();
    }
  }

  private void mockTaskStatus(final JobId jobId) throws Exception {
    final ConcurrentMap<JobId, TaskStatus> statusMap = Maps.newConcurrentMap();
    doAnswer(invocationOnMock -> {
      final TaskStatus status = (TaskStatus) invocationOnMock.getArguments()[1];
      statusMap.put(jobId, status);
      return null;
    }).when(model).setTaskStatus(eq(jobId), taskStatusCaptor.capture());

    when(model.getTaskStatus(eq(jobId))).thenReturn(statusMap.get(jobId));
  }


  @Test
  public void verifySupervisorStartsAndStopsDockerContainer() throws Exception {
    final String containerId = "deadbeef";

    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(ContainerCreation.builder().id(containerId).build());

    final ImageInfo imageInfo = mock(ImageInfo.class);
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    // Have waitContainer wait forever.
    final SettableFuture<ContainerExit> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenAnswer(futureAnswer(waitFuture));

    // Start the job
    sut.setGoal(START);

    // Verify that the container is created
    verify(docker, timeout(30000)).createContainer(containerConfigCaptor.capture(),
        containerNameCaptor.capture());
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(CREATING)
            .setContainerId(null)
            .setEnv(ENV)
            .build())
    );
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
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );
    when(docker.inspectContainer(eq(containerId))).thenReturn(runningResponse);

    verify(docker, timeout(30000)).waitContainer(containerId);
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(RUNNING)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );

    // Stop the job
    sut.setGoal(STOP);
    verify(docker, timeout(30000)).stopContainer(
        eq(containerId), eq(Supervisor.DEFAULT_SECONDS_TO_WAIT_BEFORE_KILL));

    // Change docker container state to stopped now that it was killed
    when(docker.inspectContainer(eq(containerId))).thenReturn(stoppedResponse);

    // Verify that the pulling state is signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(START)
            .setState(PULLING_IMAGE)
            .setContainerId(null)
            .setEnv(ENV)
            .build())
    );

    // Verify that the STOPPING and STOPPED states are signalled
    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(STOP)
            .setState(STOPPING)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );

    verify(model, timeout(30000)).setTaskStatus(eq(JOB.getId()),
        eq(TaskStatus.newBuilder()
            .setJob(JOB)
            .setGoal(STOP)
            .setState(STOPPED)
            .setContainerId(containerId)
            .setEnv(ENV)
            .build())
    );
  }

  @Test
  public void verifySupervisorStopsDockerContainerWithConfiguredKillTime() throws Exception {
    final String containerId = "deadbeef";

    final Job longKillTimeJob = Job.newBuilder()
        .setName(NAME)
        .setCommand(COMMAND)
        .setImage(IMAGE)
        .setVersion(VERSION)
        .setSecondsToWaitBeforeKill(30)
        .build();

    mockTaskStatus(longKillTimeJob.getId());

    final Supervisor longKillTimeSupervisor = createSupervisor(longKillTimeJob);

    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(ContainerCreation.builder().id(containerId).build());

    final ImageInfo imageInfo = mock(ImageInfo.class);
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    // Have waitContainer wait forever.
    final SettableFuture<ContainerExit> waitFuture = SettableFuture.create();
    when(docker.waitContainer(containerId)).thenAnswer(futureAnswer(waitFuture));

    // Start the job (so that a runner exists)
    longKillTimeSupervisor.setGoal(START);
    when(docker.inspectContainer(eq(containerId))).thenReturn(runningResponse);

    // This is already verified above, but it works as a hack to wait for the model/docker state
    // to converge in such a way that a setGoal(STOP) will work. :|
    verify(docker, timeout(30000)).waitContainer(containerId);

    // Stop the job
    longKillTimeSupervisor.setGoal(STOP);
    verify(docker, timeout(30000)).stopContainer(
        eq(containerId), eq(longKillTimeJob.getSecondsToWaitBeforeKill()));

    // Change docker container state to stopped now that it was killed
    when(docker.inspectContainer(eq(containerId))).thenReturn(stoppedResponse);
  }

  private String shortJobIdFromContainerName(final String containerName) {
    assertThat(containerName, startsWith(NAMESPACE + "-"));
    final String name = containerName.substring(NAMESPACE.length() + 1);
    final int lastUnderscore = name.lastIndexOf('_');
    return name.substring(0, lastUnderscore).replace('_', ':');
  }

  @Test
  public void verifySupervisorRestartsExitedContainer() throws Exception {
    final String containerId1 = "deadbeef1";
    final String containerId2 = "deadbeef2";

    final ContainerCreation createResponse1 = ContainerCreation.builder().id(containerId1).build();
    final ContainerCreation createResponse2 = ContainerCreation.builder().id(containerId2).build();

    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(createResponse1);

    final ImageInfo imageInfo = mock(ImageInfo.class);
    when(docker.inspectImage(IMAGE)).thenReturn(imageInfo);

    when(docker.inspectContainer(eq(containerId1))).thenReturn(runningResponse);

    final SettableFuture<ContainerExit> waitFuture1 = SettableFuture.create();
    final SettableFuture<ContainerExit> waitFuture2 = SettableFuture.create();
    when(docker.waitContainer(containerId1)).thenAnswer(futureAnswer(waitFuture1));
    when(docker.waitContainer(containerId2)).thenAnswer(futureAnswer(waitFuture2));

    // Start the job
    sut.setGoal(START);
    verify(docker, timeout(30000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(30000)).startContainer(eq(containerId1));
    verify(docker, timeout(30000)).waitContainer(containerId1);

    // Indicate that the container exited
    when(docker.inspectContainer(eq(containerId1))).thenReturn(stoppedResponse);
    when(docker.createContainer(any(ContainerConfig.class), any(String.class)))
        .thenReturn(createResponse2);
    when(docker.inspectContainer(eq(containerId2))).thenReturn(runningResponse);
    waitFuture1.set(ContainerExit.create(1));

    // Verify that the container was restarted
    verify(docker, timeout(30000)).createContainer(any(ContainerConfig.class), any(String.class));
    verify(docker, timeout(30000)).startContainer(eq(containerId2));
    verify(docker, timeout(30000)).waitContainer(containerId2);
  }

  public void verifyExceptionSetsTaskStatusToFailed(final Exception exception) throws Exception {
    when(docker.inspectImage(IMAGE)).thenThrow(exception);

    when(retryPolicy.delay(any(ThrottleState.class))).thenReturn(MINUTES.toMillis(1));

    // Start the job
    sut.setGoal(START);

    verify(retryPolicy, timeout(30000)).delay(any(ThrottleState.class));

    assertEquals(taskStatusCaptor.getValue().getState(), FAILED);
  }

  @Test
  public void verifyDockerExceptionSetsTaskStatusToFailed() throws Exception {
    verifyExceptionSetsTaskStatusToFailed(new DockerException("FAIL"));
  }

  @Test
  public void verifyRuntimeExceptionSetsTaskStatusToFailed() throws Exception {
    verifyExceptionSetsTaskStatusToFailed(new RuntimeException("FAIL"));
  }

  private Answer<?> futureAnswer(final SettableFuture<?> future) {
    return (Answer<Object>) invocation -> future.get();
  }

  /**
   * Verifies a fix for a NPE that is thrown when the Supervisor receives a goal of UNDEPLOY for a
   * job with gracePeriod that has never been STARTed.
   */
  @Test
  public void verifySupervisorHandlesUndeployingOfNotRunningContainerWithGracePeriod()
      throws Exception {

    final int gracePeriod = 5;
    final Job job = JOB.toBuilder()
        .setGracePeriod(gracePeriod)
        .build();

    final Supervisor sut = createSupervisor(job);

    sut.setGoal(Goal.UNDEPLOY);

    // when the NPE was thrown, the model was never updated
    verify(model, timeout(30000)).setTaskStatus(eq(job.getId()),
        argThat(is(taskStatusWithState(TaskStatus.State.STOPPING))));
    verify(model, timeout(30000)).setTaskStatus(eq(job.getId()),
        argThat(is(taskStatusWithState(TaskStatus.State.STOPPED))));

    verify(sleeper, never()).sleep(gracePeriod * 1000);
  }

  private Supervisor createSupervisor(final Job job) {
    final TaskConfig config = TaskConfig.builder()
        .namespace(NAMESPACE)
        .host("AGENT_NAME")
        .job(job)
        .envVars(ENV)
        .build();

    final TaskStatus.Builder taskStatus = TaskStatus.newBuilder()
        .setJob(job)
        .setEnv(ENV)
        .setPorts(PORTS);

    final StatusUpdater statusUpdater = new DefaultStatusUpdater(model, taskStatus);
    final TaskMonitor monitor = new TaskMonitor(
        job.getId(), FlapController.create(), statusUpdater);

    final TaskRunnerFactory runnerFactory = TaskRunnerFactory.builder()
        .registrar(registrar)
        .config(config)
        .dockerClient(docker)
        .listener(monitor)
        .build();

    return Supervisor.newBuilder()
        .setJob(job)
        .setStatusUpdater(statusUpdater)
        .setDockerClient(docker)
        .setRestartPolicy(retryPolicy)
        .setRunnerFactory(runnerFactory)
        .setMetrics(new NoopSupervisorMetrics())
        .setMonitor(monitor)
        .build();
  }

  private static Matcher<TaskStatus> taskStatusWithState(final TaskStatus.State state) {
    return new FeatureMatcher<TaskStatus, TaskStatus.State>(equalTo(state), "state", "state") {
      @Override
      protected TaskStatus.State featureValueOf(final TaskStatus actual) {
        return actual.getState();
      }
    };
  }


}
