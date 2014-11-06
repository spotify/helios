/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Optional;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class JobNamePrefixTest extends TemporaryJobsTestBase {

  private static JobPrefixFile jobPrefixFile;

  @Test
  public void testJobNamePrefix() throws Exception {
    // Create four jobs which represent these use cases:
    //  job1 - Created, deployed, locked. Simulates a job being used by another process. The
    //         job should not get undeployed or deleted since it is in use.
    //  job2 - Created, not deployed, locked. Simulates a job being used by another process. The
    //         job should not get deleted since it is in use.
    //  job3 - Created, deployed, not locked. Simulates an old job no longer in use, which should
    //         be undeployed and deleted.
    //  job4 - Created, not deployed, not locked. Simulates an old job no longer in use, which
    //         should be deleted.

    // job1 - create and deploy
    final JobId jobId1 = createJob(testJobName + "_1", testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId1, testHost1);
    // job2 - create
    final JobId jobId2 = createJob(testJobName + "_2", testJobVersion, BUSYBOX, IDLE_COMMAND);
    // job3 - create and deploy
    final JobId jobId3 = createJob(testJobName + "_3", testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId3, testHost1);
    // job4 - create
    final JobId jobId4 = createJob(testJobName + "_4", testJobVersion, BUSYBOX, IDLE_COMMAND);

    try (
        // Create prefix files for all four jobs. They will be locked by default.
        JobPrefixFile file1 = JobPrefixFile.create(jobId1.getName(), prefixDirectory);
        JobPrefixFile file2 = JobPrefixFile.create(jobId2.getName(), prefixDirectory);
        JobPrefixFile file3 = JobPrefixFile.create(jobId3.getName(), prefixDirectory);
        JobPrefixFile file4 = JobPrefixFile.create(jobId4.getName(), prefixDirectory)
    ) {
      // Release the locks of jobs 3 and 4 so they can be cleaned up
      file3.release();
      file4.release();

      assertThat(testResult(JobNamePrefixTestImpl.class), isSuccessful());

      final Map<JobId, Job> jobs = client.jobs().get();

      // Verify job1 is still deployed and the prefix file has not been deleted.
      assertThat(jobs, hasKey(jobId1));
      final JobStatus status1 = client.jobStatus(jobId1).get();
      assertThat(status1.getDeployments().size(), is(1));
      assertTrue(fileExists(prefixDirectory, jobId1.getName()));

      // Verify job2 still exists, is not deployed, and the prefix file is still there.
      assertThat(jobs, hasKey(jobId2));
      final JobStatus status2 = client.jobStatus(jobId2).get();
      assertThat(status2.getDeployments().size(), is(0));
      assertTrue(fileExists(prefixDirectory, jobId2.getName()));

      // Verify that job3 has been deleted (which means it has also been undeployed), and
      // the prefix file has been deleted.
      assertThat(jobs, not(hasKey(jobId3)));
      assertFalse(fileExists(prefixDirectory, jobId3.getName()));

      // Verify that job4 and its prefix file have been deleted.
      assertThat(jobs, not(hasKey(jobId4)));
      assertFalse(fileExists(prefixDirectory, jobId4.getName()));

      // Verify the prefix file created during the run of JobNamePrefixTest was deleted
      assertFalse(fileExists(prefixDirectory, jobPrefixFile.prefix()));
    }
  }

  private static boolean fileExists(final Path path, final String prefix) {
    return new File(path.toFile(), prefix).exists();
  }

  public static class JobNamePrefixTestImpl {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .client(client)
        .prober(new TestProber())
        .prefixDirectory(prefixDirectory.toString())
        .jobPrefix(Optional.of(testTag).get())
        .build();

    private final Date expires = new DateTime().plusHours(1).toDate();

    private TemporaryJob job1;
    private TemporaryJob job2;

    @Before
    public void setup() {
      job1 = temporaryJobs.job()
          .command(IDLE_COMMAND)
          .deploy(testHost1);

      job2 = temporaryJobs.job()
          .command(IDLE_COMMAND)
          .expires(expires)
          .deploy(testHost1);
    }

    @Test
    public void testJobPrefixFile() throws Exception {
      // Verify a default expires values was set on job1
      assertThat(job1.job().getExpires(), is(notNullValue()));

      // Verify expires was set correctly on job2
      assertThat(job2.job().getExpires(), is(equalTo(expires)));

      // Get all jobs from master to make sure values are set correctly there
      final Map<JobId, Job> jobs = client.jobs().get();

      // Verify job1 was set correctly on master
      final Job remoteJob1 = jobs.get(job1.job().getId());
      assertThat(remoteJob1, is(notNullValue()));
      assertThat(remoteJob1.getExpires(), is(equalTo(job1.job().getExpires())));

      // Verify job2 was set correctly on master
      final Job remoteJob2 = jobs.get(job2.job().getId());
      assertThat(remoteJob2, is(notNullValue()));
      assertThat(remoteJob2.getExpires(), equalTo(expires));

      // Set jobPrefixFile so we can verify it was deleted after test completed
      jobPrefixFile = temporaryJobs.jobPrefixFile();
    }
  }

}
