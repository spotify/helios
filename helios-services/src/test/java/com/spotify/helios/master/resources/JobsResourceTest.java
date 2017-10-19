/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.helios.master.resources;

import static com.spotify.helios.common.protocol.CreateJobResponse.Status.OK;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.NoopMasterMetrics;
import java.util.Map;
import org.hamcrest.CustomTypeSafeMatcher;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class JobsResourceTest {

  private final MasterModel model = mock(MasterModel.class);
  private final ImmutableSet<String> capabilities = ImmutableSet.of();
  private final Clock clock = mock(Clock.class);

  private final JobsResource resource =
      new JobsResource(model, new NoopMasterMetrics(), capabilities, clock);

  @Before
  public void setup() {
    when(clock.now()).thenReturn(new Instant(0));
  }

  @Test
  public void testListJobs() throws Exception {
    final ImmutableMap<JobId, Job> jobs = ImmutableMap.of(
        JobId.parse("foobar:1"), Job.newBuilder().build(),
        JobId.parse("foobar:2"), Job.newBuilder().build(),
        JobId.parse("foobar:3"), Job.newBuilder().build()
    );

    when(model.getJobs()).thenReturn(jobs);

    assertThat(resource.list("", ""), is(jobs));
  }

  @Test
  public void testListJobsWithJobNameFilter() throws Exception {
    final JobId jobId1 = JobId.parse("foobar:1");
    final JobId jobId2 = JobId.parse("foobar:2");
    final Job job1 = Job.newBuilder().build();
    final Job job2 = Job.newBuilder().build();

    final ImmutableMap<JobId, Job> jobs = ImmutableMap.of(
        jobId1, job1,
        jobId2, job2,
        JobId.parse("blah:3"), Job.newBuilder().build(),
        JobId.parse("buzz:3"), Job.newBuilder().build()
    );

    when(model.getJobs()).thenReturn(jobs);

    assertThat(resource.list("foobar", ""), is(
        ImmutableMap.of(
            jobId1, job1,
            jobId2, job2
        )
    ));
  }

  @Test
  public void testListJobsWithHostNameFilter() throws Exception {
    // one host matches this name pattern
    final String namePattern = "foo";
    when(model.listHosts(namePattern)).thenReturn(ImmutableList.of("foobar.example.net"));

    final JobId jobId1 = JobId.parse("foobar:1");
    final JobId jobId2 = JobId.parse("foobat:2");

    final Job job1 = Job.newBuilder().build();
    final Job job2 = Job.newBuilder().build();

    // and it has two jobs deployed to it
    final HostStatus hostStatus = mockHostStatus(ImmutableMap.of(
        jobId1, Deployment.of(jobId1, Goal.START),
        jobId2, Deployment.of(jobId2, Goal.START)
    ));
    when(model.getHostStatus("foobar.example.net")).thenReturn(hostStatus);

    when(model.getJob(jobId1)).thenReturn(job1);
    when(model.getJob(jobId2)).thenReturn(job2);

    assertThat(resource.list("", namePattern), is(
        ImmutableMap.of(
            jobId1, job1,
            jobId2, job2
        )
    ));
  }

  @Test
  public void testListJobsWithMultipleDeployments() throws Exception {
    // two hosts match this name patterns
    final String namePattern = "foo";
    when(model.listHosts(namePattern))
        .thenReturn(ImmutableList.of("foobar.example.net", "barfoo.example.net"));

    final JobId jobId1 = JobId.parse("foobar:1");
    final JobId jobId2 = JobId.parse("foobat:2");

    final Job job1 = Job.newBuilder().build();
    final Job job2 = Job.newBuilder().build();

    // and they have two jobs deployed
    final HostStatus hostStatus = mockHostStatus(ImmutableMap.of(
        jobId1, Deployment.of(jobId1, Goal.START),
        jobId2, Deployment.of(jobId2, Goal.START)
    ));
    when(model.getHostStatus("foobar.example.net")).thenReturn(hostStatus);
    when(model.getHostStatus("barfoo.example.net")).thenReturn(hostStatus);

    when(model.getJob(jobId1)).thenReturn(job1);
    when(model.getJob(jobId2)).thenReturn(job2);

    assertThat(resource.list("", namePattern), is(
        ImmutableMap.of(
            jobId1, job1,
            jobId2, job2
        )
    ));
  }

  @Test
  public void testCreateJobWithNoRolloutOptions() throws Exception {
    final JobId jobId = JobId.parse("foobar:1");
    final Job job = Job.newBuilder()
        .setName("foobar")
        .setVersion("1")
        .setImage("busybox:latest")
        .setCreatingUser("user1")
        .setCreated(0L)
        .build();

    final CreateJobResponse jobResponse = resource.post(job, "user1");
    assertThat(jobResponse,
        new CustomTypeSafeMatcher<CreateJobResponse>("CreateJobResponse that is OK") {
          @Override
          protected boolean matchesSafely(final CreateJobResponse response) {
            return response.getStatus() == OK
                   && response.getErrors().isEmpty()
                   && response.getId().contains(jobId.toString());
          }
        }
    );

    verify(model).addJob(job);
  }

  @Test
  public void testCreateJobWithPartialRolloutOptions() throws Exception {
    final JobId jobId = JobId.parse("foobar:1");
    final Job job = Job.newBuilder()
        .setName("foobar")
        .setVersion("1")
        .setImage("busybox:latest")
        .setRolloutOptions(RolloutOptions.newBuilder()
            .setTimeout(null)
            .setParallelism(2)
            .setMigrate(null)
            .setOverlap(true)
            .setToken(null)
            .setIgnoreFailures(null)
            .build())
        .setCreatingUser("user1")
        .setCreated(0L)
        .build();

    final CreateJobResponse jobResponse = resource.post(job, "user1");
    assertThat(jobResponse,
        new CustomTypeSafeMatcher<CreateJobResponse>("CreateJobResponse that is OK") {
          @Override
          protected boolean matchesSafely(final CreateJobResponse response) {
            return response.getStatus() == OK
                   && response.getErrors().isEmpty()
                   && response.getId().contains(jobId.toString());
          }
        }
    );

    verify(model).addJob(job);
  }

  private static HostStatus mockHostStatus(Map<JobId, Deployment> jobs) {
    final HostStatus hostStatus = mock(HostStatus.class);
    when(hostStatus.getJobs()).thenReturn(jobs);
    return hostStatus;
  }
}
