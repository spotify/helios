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

package com.spotify.helios.master.reaper;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.master.MasterModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class OldJobReaperTest {

  @Rule
  public TestName name = new TestName();

  private static final long RETENTION_DAYS = 1;
  private static final Job DUMMY_JOB = Job.newBuilder().build();

  private static class Datapoint {

    private final Job job;
    private final List<TaskStatusEvent> history;
    private final Map<String, Deployment> deployments;
    private final JobStatus jobStatus;

    private Datapoint(
        final String jobName,
        final Map<String, Deployment> deployments,
        final List<TaskStatusEvent> history) {
      this(jobName, deployments, history, null);
    }

    private Datapoint(
        final String jobName,
        final Map<String, Deployment> deployments,
        final List<TaskStatusEvent> history,
        final Long created) {
      final Job.Builder builder = Job.newBuilder().setName(jobName);
      if (created != null) {
        builder.setCreated(created);
      }
      this.job = builder.build();
      this.history = ImmutableList.copyOf(history);
      this.deployments = ImmutableMap.copyOf(deployments);
      this.jobStatus = JobStatus.newBuilder().setDeployments(this.deployments).build();
    }

    public Job getJob() {
      return job;
    }

    JobId getJobId() {
      return job.getId();
    }

    List<TaskStatusEvent> getHistory() {
      return this.history;
    }

    JobStatus getJobStatus() {
      return this.jobStatus;
    }
  }

  private List<TaskStatusEvent> events(final List<Long> timestamps) {
    final ImmutableList.Builder<TaskStatusEvent> builder = ImmutableList.builder();

    // First sort by timestamps ascending
    final List<Long> copy = Lists.newArrayList(timestamps);
    Collections.sort(copy);

    for (final Long timestamp : timestamps) {
      final TaskStatus taskStatus = TaskStatus.newBuilder()
          .setJob(DUMMY_JOB)
          .setGoal(Goal.START)
          .setState(State.RUNNING)
          .build();
      builder.add(new TaskStatusEvent(taskStatus, timestamp, ""));
    }

    return builder.build();
  }

  private Map<String, Deployment> deployments(final JobId jobId, final int numHosts) {
    final ImmutableMap.Builder<String, Deployment> builder = ImmutableMap.builder();

    for (int i = 0; i < numHosts; i++) {
      builder.put("host" + i, Deployment.of(jobId, Goal.START));
    }
    return builder.build();
  }

  /**
   * A job not deployed, with history, and last used too long ago should BE reaped.
   */
  @Test
  public void jobNotDeployedWithHistoryLastUsedTooLongAgoReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(22))));
    testReap(datapoint, masterModel, true);
  }

  /**
   * A job not deployed, with history, and last used recently should NOT BE reaped.
   */
  @Test
  public void jobNotDeployedWithHistoryLastUsedRecentlyNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(40))));
    testReap(datapoint, masterModel, false);
  }

  /**
   * A job not deployed, without history, and without a creation date should BE reaped.
   */
  @Test
  public void jobNotDeployedWithoutHistoryWithCreateDateReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(name.getMethodName(), emptyMap(), emptyList());
    testReap(datapoint, masterModel, true);
  }

  /**
   * A job not deployed, without history, and created before retention time should BE reaped.
   */
  @Test
  public void jobNotDeployedWithoutHistoryCreateBeforeRetentionReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        emptyList(),
        HOURS.toMillis(23));
    testReap(datapoint, masterModel, true);
  }

  /**
   * A job not deployed, without history, created after retention time should NOT BE reaped.
   */
  @Test
  public void jobNotDeployedWithoutHistoryCreateAfterRetentionNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        emptyList(),
        HOURS.toMillis(25));
    testReap(datapoint, masterModel, false);
  }

  /**
   * A job deployed and without history should NOT BE reaped.
   */
  @Test
  public void jobDeployedWithoutHistoryNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        deployments(JobId.fromString(name.getMethodName()), 2),
        emptyList());
    testReap(datapoint, masterModel, false);
  }

  /**
   * A job deployed, with history, and last used too long ago should NOT BE reaped.
   */
  @Test
  public void jobDeployedWithHistoryLastUsedTooLongAgoNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        deployments(JobId.fromString(name.getMethodName()), 3),
        events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(22))));
    testReap(datapoint, masterModel, false);
  }

  /**
   * A job deployed, with history, and last used recently should NOT BE reaped.
   */
  @Test
  public void jobDeployedWithHistoryLastUsedRecentlyNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        deployments(JobId.fromString(name.getMethodName()), 3),
        events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(40))));
    testReap(datapoint, masterModel, false);
  }

  /**
   * A job not deployed, with history, last used too long ago and part of a deployment group
   * should NOT BE reaped.
   */
  @Test
  public void jobDeployedWithHistoryLastUsedTooLongAgoInDgNotReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);

    when(masterModel.getDeploymentGroups())
        .thenReturn(ImmutableMap.of(
            "testdg",
            new DeploymentGroup("name", new ArrayList<>(), JobId.fromString(name.getMethodName()),
                RolloutOptions.getDefault(), DeploymentGroup.RollingUpdateReason.MANUAL)));

    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        events(ImmutableList.of(HOURS.toMillis(2), HOURS.toMillis(3))));

    testReap(datapoint, masterModel, false);
  }

  /**
   * A job not deployed, with history, last used too long ago and part of a deployment group
   * should BE reaped.
   */
  @Test
  public void jobDeployedWithHistoryLastUsedTooLongAgoNotInDgReaped() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);

    when(masterModel.getDeploymentGroups())
        .thenReturn(ImmutableMap.of(
            "testdg",
            new DeploymentGroup("name", new ArrayList<>(), JobId.fromString("framazama"),
                RolloutOptions.getDefault(), DeploymentGroup.RollingUpdateReason.MANUAL)));

    final Datapoint datapoint = new Datapoint(
        name.getMethodName(),
        emptyMap(),
        events(ImmutableList.of(HOURS.toMillis(2), HOURS.toMillis(3))));

    testReap(datapoint, masterModel, true);
  }

  private void testReap(
      final Datapoint datapoint,
      final MasterModel masterModel,
      final boolean expectReap) throws Exception {
    final Clock clock = mock(Clock.class);
    when(clock.now()).thenReturn(new Instant(HOURS.toMillis(48)));

    when(masterModel.getJobs())
        .thenReturn(ImmutableMap.of(datapoint.getJobId(), datapoint.getJob()));

    when(masterModel.getJobHistory(datapoint.getJobId())).thenReturn(datapoint.getHistory());
    when(masterModel.getJobStatus(datapoint.getJobId())).thenReturn(datapoint.getJobStatus());

    final OldJobReaper reaper = new OldJobReaper(masterModel, RETENTION_DAYS, clock, 100, 0);
    reaper.startAsync().awaitRunning();

    // Wait 100ms to give the reaper enough time to process all the jobs before verifying :(
    Thread.sleep(100);

    if (expectReap) {
      verify(masterModel, timeout(500)).removeJob(datapoint.getJobId(), Job.EMPTY_TOKEN);
    } else {
      verify(masterModel, never()).removeJob(datapoint.getJobId(), Job.EMPTY_TOKEN);
    }
  }
}
