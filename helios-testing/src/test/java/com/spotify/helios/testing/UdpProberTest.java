/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

/**
 * Tests that a UDP port in a TemporaryJob can be probed.
 */
public class UdpProberTest extends TemporaryJobsTestBase {

  @Test
  public void test() throws Exception {
    assertThat(testResult(UdpProberTestImpl.class), isSuccessful());
  }

  public static class UdpProberTestImpl {

    private TemporaryJob job;

    @Rule
    public final TemporaryJobs temporaryJobs = temporaryJobsBuilder()
        .client(client)
        .prober(new TestProber())
        .build();

    @Before
    public void setup() {
      job = temporaryJobs.job()
          .image(ALPINE)
          .command(asList("nc", "-p", "4711", "-lu"))
          .port("default", 4711, "udp")
          .deploy(testHost1);
    }

    @After
    public void tearDown() {
      // The TemporaryJobs Rule above doesn't undeploy the job for some reason...
      job.undeploy();
    }

    @Test
    public void test() throws Exception {
      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 1, jobs.size());
      for (final Job job : jobs.values()) {
        assertEquals("wrong job running", ALPINE, job.getImage());
      }
    }
  }
}
