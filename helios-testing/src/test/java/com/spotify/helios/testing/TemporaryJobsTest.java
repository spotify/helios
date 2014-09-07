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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.system.SystemTestBase;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.testing.Jobs.getJobDescription;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.hasFailureContaining;
import static org.junit.experimental.results.ResultMatchers.hasSingleFailureContaining;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class TemporaryJobsTest extends SystemTestBase {

  // These static fields exist as a way for nested tests to access non-static fields and methods in
  // SystemTestBase. This is a bit ugly, but we can't pass the values to FakeTest, because we don't
  // instantiate it, JUnit does in the PrintableResult.testResult method. And since JUnit
  // instantiates it, it must be a static class, which means it can't access the non-static fields
  // in SystemTestBase.
  private static HeliosClient client;
  private static String testHost1;
  private static String testHost2;
  private static JobPrefixFile jobPrefixFile;
  private static Path prefixDirectory;

  private static final class TestProber extends DefaultProber {

    @Override
    public boolean probe(final String host, final int port) {
      // Probe for ports where docker is running instead of on the mock testHost address
      assertEquals(testHost1, host);
      return super.probe(DOCKER_HOST.address(), port);
    }
  }

  public static class SimpleTest {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .build();

    private TemporaryJob job1;

    @Before
    public void setup() {
      job1 = temporaryJobs.job()
          .image(BUSYBOX)
          .command("nc", "-p", "4711", "-lle", "cat")
          .port("echo", 4711)
          .deploy(testHost1);
    }

    @Test
    public void testDeployment() throws Exception {
      // Verify that it is possible to deploy additional jobs during test
      temporaryJobs.job()
          .image(BUSYBOX)
          .command(IDLE_COMMAND)
          .host(testHost1)
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (Job job : jobs.values()) {
        assertEquals("wrong job running", BUSYBOX, job.getImage());
      }

      //verify address and addresses return valid HostAndPort objects
      assertEquals("wrong host", testHost1, job1.address("echo").getHostText());
      assertEquals("wrong host", testHost1, getOnlyElement(job1.addresses("echo")).getHostText());

      ping(DOCKER_HOST.address(), job1.port(testHost1, "echo"));
    }

    @Test
    public void testRandomHost() throws Exception {
      temporaryJobs.job()
          .image(BUSYBOX)
          .command("sh", "-c", "while :; do sleep 5; done")
          .hostFilter(".+")
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (Job job : jobs.values()) {
        assertEquals("wrong job running", BUSYBOX, job.getImage());
      }

      ping(DOCKER_HOST.address(), job1.port(testHost1, "echo"));
    }

    @Test
    public void testSpecificHost() throws Exception {
      final TemporaryJob job = temporaryJobs.job()
          .image(BUSYBOX)
          .command(IDLE_COMMAND)
          .hostFilter(testHost2)
          .deploy();

      final JobStatus status = client.jobStatus(job.job().getId()).get(15, SECONDS);
      final Map<String, Deployment> deployments = status.getDeployments();
      assertThat(deployments.keySet(), contains(testHost2));
    }

    public void testDefaultLocalHostFilter() throws Exception {
      temporaryJobs.job()
          .image(BUSYBOX)
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    @Test(expected = AssertionError.class)
    public void testExceptionWithBadHostFilter() throws Exception {
      // Shouldn't be able to deploy if filter doesn't match any hosts
      temporaryJobs.job()
          .image(BUSYBOX)
          .command("sh", "-c", "while :; do sleep 5; done")
          .hostFilter("THIS_FILTER_SHOULDNT_MATCH_ANY_HOST")
          .deploy();
    }

    @Test
    public void testImageFromBuild() {
      temporaryJobs.job()
          .imageFromBuild()
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    private void ping(final String host, final int port) throws Exception {
      try (final Socket s = new Socket(host, port)) {
        byte[] ping = "ping".getBytes(UTF_8);
        s.getOutputStream().write(ping);
        final byte[] pong = new byte[4];
        final int n = s.getInputStream().read(pong);
        assertEquals(4, n);
        assertArrayEquals(ping, pong);
      }
    }
  }

  public static class BadTest {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .build();

    @SuppressWarnings("unused")
    private TemporaryJob job2 = temporaryJobs.job()
        .image("base")
        .deploy(testHost1);

    @Test
    public void testFail() throws Exception {
      fail();
    }
  }

  public static class JobNamePrefixTest {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .prefixDirectory(prefixDirectory.toString())
        .build();

    private final Date expires = new DateTime().plusHours(1).toDate();

    private TemporaryJob job1;
    private TemporaryJob job2;

    @Before
    public void setup() {
      job1 = temporaryJobs.job()
          .image(BUSYBOX)
          .command(IDLE_COMMAND)
          .deploy(testHost1);

      job2 = temporaryJobs.job()
          .image(BUSYBOX)
          .command(IDLE_COMMAND)
          .expires(expires)
          .deploy(testHost1);
    }

    @Test public void testJobPrefixFile() throws Exception {
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

  @Test
  public void testRule() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost1 = testHost() + "1";
    testHost2 = testHost() + "2";
    startDefaultAgent(testHost1);
    startDefaultAgent(testHost2);

    awaitHostStatus(client, testHost1, UP, LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, testHost2, UP, LONG_WAIT_MINUTES, MINUTES);

    assertThat(testResult(SimpleTest.class), isSuccessful());
    assertTrue("jobs are running that should not be",
               client.jobs().get(15, SECONDS).isEmpty());
  }

  @Test
  public void verifyJobFailsWhenCalledBeforeTestRun() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost1 = testHost();
    assertThat(testResult(BadTest.class),
               hasFailureContaining("deploy() must be called in a @Before or in the test method"));
  }

  @Test
  public void testJobNamePrefix() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost1 = testHost();
    prefixDirectory = temporaryFolder.newFolder().toPath();
    startDefaultAgent(testHost1);
    awaitHostStatus(client, testHost1, UP, LONG_WAIT_MINUTES, MINUTES);

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

      assertThat(testResult(JobNamePrefixTest.class), isSuccessful());

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

  public static class TempJobFailureTest {
    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .prefixDirectory(prefixDirectory.toString())
        .build();

    @Test
    public void testThatThisFailsQuickly() throws InterruptedException {
      temporaryJobs.job()
          .image(BUSYBOX)
          .command("false")
          .deploy(testHost1);
      Thread.sleep(Jobs.TIMEOUT_MILLIS);
    }
  }

  @Test
  public void testDeploymentFailure() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost1 = testHost();
    prefixDirectory = temporaryFolder.newFolder().toPath();
    startDefaultAgent(testHost1);
    awaitHostStatus(client, testHost1, UP, LONG_WAIT_MINUTES, MINUTES);

    final long start = System.currentTimeMillis();
    assertThat(testResult(TempJobFailureTest.class), hasSingleFailureContaining("AssertionError: Unexpected job state"));
    final long end = System.currentTimeMillis();
    assertTrue("Test should not time out", (end-start) < Jobs.TIMEOUT_MILLIS);
  }

  @Test
  public void testGetJobDescription() {
    final Job job = Job.newBuilder()
        .setImage(BUSYBOX)
        .setName("testGetJobDescription")
        .setVersion("1")
        .build();
    final String shortHash = job.getId().getHash().substring(0, 7);

    // Simple test to verify the job description contains the image name and a shortened job hash.
    assertThat(getJobDescription(job),
               both(startsWith(BUSYBOX)).and(containsString(shortHash)));
  }

  private static boolean fileExists(final Path path, final String prefix) {
    return new File(path.toFile(), prefix).exists();
  }

}
