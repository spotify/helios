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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExistingHeliosDeploymentTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HeliosClient client;
  private Undeployer undeployer;

  @Before
  public void before() {
    client = mock(HeliosClient.class);
    undeployer = mock(Undeployer.class);
  }

  @Test
  public void testRemoveOldJobs() throws Exception {
    final File file1 = folder.newFile("foo");
    final File file2 = folder.newFile("bar.tmp");
    final File dir = folder.newFolder("baz");
    final File file3 = folder.newFile("qux");
    final List<File> files = ImmutableList.of(file1, file2, dir, file3);

    final JobId jobId1 = new JobId("foo", "1");
    final JobId jobId2 = new JobId("foobar", "2");
    final JobId jobId3 = new JobId("foobar", "3");
    final JobId jobId4 = new JobId("bar", "4");
    final JobId jobId5 = new JobId("baz", "5");
    final JobId jobId6 = new JobId("qux", "6");

    final Job job1 = Job.newBuilder().setName("foo").setVersion("1").build();
    final Job job2 = Job.newBuilder().setName("foobar").setVersion("2").build();
    final Job job3 = Job.newBuilder().setName("foobar").setVersion("3").build();
    final Job job4 = Job.newBuilder().setName("bar").setVersion("4").build();
    final Job job5 = Job.newBuilder().setName("baz").setVersion("5").build();
    final Job job6 = Job.newBuilder().setName("qux").setVersion("6").build();

    final Map<String, Deployment> deployments1 = ImmutableMap.of(
        "host1", Deployment.of(jobId1, Goal.START));
    final Map<String, Deployment> deployments2 = ImmutableMap.of(
        "host2a", Deployment.of(jobId2, Goal.START), "host2b", Deployment.of(jobId2, Goal.START));
    final Map<String, Deployment> deployments4 = ImmutableMap.of(
        "host4", Deployment.of(jobId4, Goal.START));
    final Map<String, Deployment> deployments5 = ImmutableMap.of(
        "host5", Deployment.of(jobId5, Goal.START));
    final Map<String, Deployment> deployments6 = ImmutableMap.of(
        "host6", Deployment.of(jobId6, Goal.START));

    final JobStatus jobStatus1 = JobStatus.newBuilder().setDeployments(deployments1).build();
    final JobStatus jobStatus2 = JobStatus.newBuilder().setDeployments(deployments2).build();
    final JobStatus jobStatus4 = JobStatus.newBuilder().setDeployments(deployments4).build();
    final JobStatus jobStatus5 = JobStatus.newBuilder().setDeployments(deployments5).build();
    final JobStatus jobStatus6 = JobStatus.newBuilder().setDeployments(deployments6).build();

    final Map<JobId, Job> jobs = ImmutableMap.<JobId, Job>builder()
        .put(jobId1, job1)
        .put(jobId2, job2)
        .put(jobId3, job3)
        .put(jobId4, job4)
        .put(jobId5, job5)
        .put(jobId6, job6)
        .build();

    when(client.jobs()).thenReturn(Futures.immediateFuture(jobs));
    when(client.jobStatus(jobId1)).thenReturn(Futures.immediateFuture(jobStatus1));
    when(client.jobStatus(jobId2)).thenReturn(Futures.immediateFuture(jobStatus2));
    // Cause an NPE and check we keep processing later files
    when(client.jobStatus(jobId3)).thenReturn(Futures.immediateFuture((JobStatus) null));
    when(client.jobStatus(jobId3)).thenReturn(Futures.immediateFuture(jobStatus4));
    when(client.jobStatus(jobId5)).thenReturn(Futures.immediateFuture(jobStatus5));
    when(client.jobStatus(jobId6)).thenReturn(Futures.immediateFuture(jobStatus6));

    when(undeployer.undeploy(any(Job.class), anyListOf(String.class)))
        .thenReturn(Collections.<AssertionError>emptyList());

    final ExistingHeliosDeployment helios = ExistingHeliosDeployment.newBuilder()
        .undeployer(undeployer)
        .heliosClient(client)
        .build();

    helios.removeOldJobs(files);

    verify(undeployer).undeploy(job1, singletonList("host1"));
    verify(undeployer).undeploy(job2, ImmutableList.of("host2a", "host2b"));
    // job3 shouldn't be undeployed because retrieving its status caused an NPE
    verify(undeployer, never()).undeploy(job3, singletonList("host3"));
    // job4 and job5 shouldn't be undeployed because no prefix file that doesn't end in ".tmp"
    // starts their names
    verify(undeployer, never()).undeploy(job4, singletonList("host4"));
    verify(undeployer, never()).undeploy(job5, singletonList("host5"));
    verify(undeployer).undeploy(job6, singletonList("host6"));
  }
}
