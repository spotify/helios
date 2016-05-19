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

import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import com.google.common.base.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.Socket;
import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class SimpleTest extends TemporaryJobsTestBase {

  @Test
  public void simpleTest() throws Exception {
    assertThat(testResult(SimpleTestImpl.class), isSuccessful());
    assertTrue("jobs are running that should not be",
               client.jobs().get(15, SECONDS).isEmpty());
  }

  public static class SimpleTestImpl {

    @Rule
    public final TemporaryJobs temporaryJobs = temporaryJobsBuilder()
        .client(client)
        .prober(new TestProber())
        .jobDeployedMessageFormat(
            "Logs Link: http://${host}:8150/${name}%3A${version}%3A${hash}?cid=${containerId}")
        .jobPrefix(Optional.of(testTag).get())
        .deployTimeoutMillis(MINUTES.toMillis(3))
        .build();

    private TemporaryJob job1;

    @Before
    public void setup() {
      job1 = temporaryJobs.job()
          .command("nc", "-p", "4711", "-lle", "cat")
          .port("echo", 4711)
          .deploy(testHost1);
    }

    @Test
    public void testDeployment() throws Exception {
      // Verify that it is possible to deploy additional jobs during test
      temporaryJobs.job()
          .command(IDLE_COMMAND)
          .host(testHost1)
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (final Job job : jobs.values()) {
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
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (final Job job : jobs.values()) {
        assertEquals("wrong job running", BUSYBOX, job.getImage());
      }

      ping(DOCKER_HOST.address(), job1.port(testHost1, "echo"));
    }

    @Test
    public void testSpecificHost() throws Exception {
      final TemporaryJob job = temporaryJobs.job()
          .command(IDLE_COMMAND)
          .hostFilter(testHost2)
          .deploy();

      final JobStatus status = client.jobStatus(job.job().getId()).get(15, SECONDS);
      final Map<String, Deployment> deployments = status.getDeployments();
      assertThat(deployments.keySet(), contains(testHost2));
    }

    @Test
    public void testManualUndeploy() throws Exception {
      final TemporaryJob job = temporaryJobs.job()
          .command(IDLE_COMMAND)
          .deploy();

      job.undeploy();

      final JobStatus status = client.jobStatus(job.job().getId()).get(15, SECONDS);
      assertNull("job still exists", status);
    }

    @Test
    public void testDefaultLocalHostFilter() throws Exception {
      temporaryJobs.job()
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    @Test(expected = AssertionError.class)
    public void testExceptionWithBadHostFilter() throws Exception {
      // Shouldn't be able to deploy if filter doesn't match any hosts
      temporaryJobs.job()
          .command("sh", "-c", "while :; do sleep 5; done")
          .hostFilter("THIS_FILTER_SHOULDNT_MATCH_ANY_HOST")
          .deploy();
    }

    @Test
    public void testImageFromBuild() {
      temporaryJobs.job()
          .image(BUSYBOX)
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    @Test
    public void testVolume() {
      temporaryJobs.job()
          .volume(System.getProperty("user.dir") + "/helios-testing/src/test/resources/helios.conf",
                  "/helios.conf")
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    private void ping(final String host, final int port) throws Exception {
      try (final Socket s = new Socket(host, port)) {
        final byte[] ping = "ping".getBytes(UTF_8);
        s.getOutputStream().write(ping);
        final byte[] pong = new byte[4];
        final int n = s.getInputStream().read(pong);
        assertEquals(4, n);
        assertArrayEquals(ping, pong);
      }
    }
  }


}
