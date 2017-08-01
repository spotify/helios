/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli.command;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

public class JobStatusCommandTest {

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private JobStatusCommand command;

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("list");
    command = new JobStatusCommand(subparser);

    // defaults for flags
    when(options.getString("job")).thenReturn(null);
    when(options.getString("host")).thenReturn("");
  }

  @Test
  public void testFilterJob() throws Exception {
    when(options.getString("job")).thenReturn("foo");

    final JobId jobId1 = JobId.parse("foo:bar");
    final JobId jobId2 = JobId.parse("foo:bat");
    final Job job1 = jobWithId(jobId1);
    final Job job2 = jobWithId(jobId2);

    final Map<JobId, Job> jobs = ImmutableMap.of(
        jobId1, job1,
        jobId2, job2
    );
    when(client.jobs("foo", "")).thenReturn(immediateFuture(jobs));

    final TaskStatus taskStatus1 = TaskStatus.newBuilder()
        .setGoal(Goal.START)
        .setJob(job1)
        .setState(TaskStatus.State.RUNNING)
        .build();

    final TaskStatus taskStatus2 = TaskStatus.newBuilder()
        .setGoal(Goal.START)
        .setJob(job2)
        .setState(TaskStatus.State.RUNNING)
        .build();

    final Map<JobId, JobStatus> statusMap = ImmutableMap.of(
        jobId1, JobStatus.newBuilder()
            .setDeployments(ImmutableMap.of("host1", Deployment.of(jobId1, Goal.START)))
            .setJob(job1)
            .setTaskStatuses(ImmutableMap.of("host1", taskStatus1))
            .build(),
        jobId2, JobStatus.newBuilder()
            .setDeployments(ImmutableMap.of("host2", Deployment.of(jobId2, Goal.START)))
            .setJob(job2)
            .setTaskStatuses(ImmutableMap.of("host2", taskStatus2))
            .build()
    );
    when(client.jobStatuses(jobs.keySet())).thenReturn(immediateFuture(statusMap));

    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);

    assertThat(baos.toString().split("\n"), Matchers.arrayContaining(
        "JOB ID             HOST      GOAL     STATE      CONTAINER ID    PORTS    ",
        "foo:bar:6123457    host1.    START    RUNNING                             ",
        "foo:bat:c8aa21a    host2.    START    RUNNING                             "
    ));

  }

  @Test
  public void testFilterJobAndHost() throws Exception {
    when(options.getString("job")).thenReturn("foo");
    when(options.getString("host")).thenReturn("host");

    final JobId jobId1 = JobId.parse("foo:bar");
    final JobId jobId2 = JobId.parse("foo:bat");
    final Job job1 = jobWithId(jobId1);
    final Job job2 = jobWithId(jobId2);

    final Map<JobId, Job> jobs = ImmutableMap.of(
        jobId1, job1,
        jobId2, job2
    );
    when(client.jobs("foo", "host")).thenReturn(immediateFuture(jobs));

    final TaskStatus taskStatus1 = TaskStatus.newBuilder()
        .setGoal(Goal.START)
        .setJob(job1)
        .setState(TaskStatus.State.RUNNING)
        .build();

    final TaskStatus taskStatus2 = TaskStatus.newBuilder()
        .setGoal(Goal.START)
        .setJob(job2)
        .setState(TaskStatus.State.RUNNING)
        .build();

    final Map<JobId, JobStatus> statusMap = ImmutableMap.of(
        jobId1, JobStatus.newBuilder()
            .setDeployments(ImmutableMap.of("host1", Deployment.of(jobId1, Goal.START)))
            .setJob(job1)
            .setTaskStatuses(ImmutableMap.of("host1", taskStatus1))
            .build(),
        jobId2, JobStatus.newBuilder()
            .setDeployments(ImmutableMap.of("host2", Deployment.of(jobId2, Goal.START)))
            .setJob(job2)
            .setTaskStatuses(ImmutableMap.of("host2", taskStatus2))
            .build()
    );
    when(client.jobStatuses(jobs.keySet())).thenReturn(immediateFuture(statusMap));

    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);

    assertThat(baos.toString().split("\n"), Matchers.arrayContaining(
        "JOB ID             HOST      GOAL     STATE      CONTAINER ID    PORTS    ",
        "foo:bar:6123457    host1.    START    RUNNING                             ",
        "foo:bat:c8aa21a    host2.    START    RUNNING                             "
    ));
  }

  private static Job jobWithId(final JobId jobId) {
    return Job.newBuilder()
        .setName(jobId.getName())
        .setVersion(jobId.getVersion())
        .setHash(jobId.getHash())
        .build();
  }
}
