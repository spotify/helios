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
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.master.MasterModel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.joda.time.Instant;
import org.junit.Test;

public class OldJobReaperTest {

  private static final long RETENTION_DAYS = 1;
  private static final Job DUMMY_JOB = Job.newBuilder().build();

  private static class Datapoint {

    private final Job job;
    private final List<TaskStatusEvent> history;
    private final Map<String, Deployment> deployments;
    private final JobStatus jobStatus;
    private final boolean expectReap;

    private Datapoint(final String jobName, final Map<String, Deployment> deployments,
                      final List<TaskStatusEvent> history, final boolean expectReap) {
      this(jobName, deployments, history, null, expectReap);
    }

    private Datapoint(final String jobName, final Map<String, Deployment> deployments,
                      final List<TaskStatusEvent> history, final Long created,
                      final boolean expectReap) {
      final Job.Builder builder = Job.newBuilder().setName(jobName);
      if (created != null) {
        builder.setCreated(created);
      }
      this.job = builder.build();
      this.history = ImmutableList.copyOf(history);
      this.deployments = ImmutableMap.copyOf(deployments);
      this.jobStatus = JobStatus.newBuilder().setDeployments(this.deployments).build();
      this.expectReap = expectReap;
    }

    public Job getJob() {
      return job;
    }

    public JobId getJobId() {
      return job.getId();
    }

    public List<TaskStatusEvent> getHistory() {
      return this.history;
    }

    public JobStatus getJobStatus() {
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

  @Test
  public void testOldJobReaper() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Clock clock = mock(Clock.class);
    when(clock.now()).thenReturn(new Instant(HOURS.toMillis(48)));

    final List<Datapoint> datapoints = Lists.newArrayList(
        // A job not deployed, with history, and last used too long ago should BE reaped
        new Datapoint("job1", emptyMap(),
            events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(22))), true),
        // A job not deployed, with history, and last used recently should NOT BE reaped
        new Datapoint("job2", emptyMap(),
            events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(40))), false),
        // A job not deployed, without history, and without a creation date should BE reaped
        new Datapoint("job3", emptyMap(), emptyList(), true),
        // A job not deployed, without history, and created before retention time should BE reaped
        new Datapoint("job4", emptyMap(), emptyList(), HOURS.toMillis(23), true),
        // A job not deployed, without history, created after retention time should NOT BE reaped
        new Datapoint("job5", emptyMap(), emptyList(), HOURS.toMillis(25), false),
        // A job deployed and without history should NOT BE reaped
        new Datapoint("job6", deployments(JobId.fromString("job6"), 2), emptyList(), false),
        // A job deployed, with history, and last used too long ago should NOT BE reaped
        new Datapoint("job7", deployments(JobId.fromString("job7"), 3),
            events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(22))), false),
        // A job deployed, with history, and last used recently should NOT BE reaped
        new Datapoint("job8", deployments(JobId.fromString("job8"), 3),
            events(ImmutableList.of(HOURS.toMillis(20), HOURS.toMillis(40))), false)
    );

    when(masterModel.getJobs()).thenReturn(
        datapoints.stream().collect(Collectors.toMap(Datapoint::getJobId, Datapoint::getJob)));

    for (final Datapoint datapoint : datapoints) {
      when(masterModel.getJobHistory(datapoint.getJobId())).thenReturn(datapoint.getHistory());
      when(masterModel.getJobStatus(datapoint.getJobId())).thenReturn(datapoint.getJobStatus());
    }

    final OldJobReaper reaper = new OldJobReaper(masterModel, RETENTION_DAYS, clock, 100, 0);
    reaper.startAsync().awaitRunning();

    // Wait one second to give the reaper enough time to process all the jobs before verifying :(
    Thread.sleep(1000);

    for (final Datapoint datapoint : datapoints) {
      if (datapoint.expectReap) {
        verify(masterModel, timeout(500)).removeJob(datapoint.getJobId(), Job.EMPTY_TOKEN);
      } else {
        verify(masterModel, never()).removeJob(datapoint.getJobId(), Job.EMPTY_TOKEN);
      }
    }
  }
}
