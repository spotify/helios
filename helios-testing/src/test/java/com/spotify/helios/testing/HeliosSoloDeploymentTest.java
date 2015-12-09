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


import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class HeliosSoloDeploymentTest {

  @Test
  public void heliosSoloDeploymentTest() {
    assertThat(testResult(HeliosSoloDeploymentTestImpl.class), isSuccessful());
  }


  public static class HeliosSoloDeploymentTestImpl {

    public static final String BUSYBOX = "busybox:latest";
    public static final List<String> IDLE_COMMAND = asList(
            "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");

    private TemporaryJob soloJob;

    // TODO(negz): We want one deployment per test run, not one per test class.
    @ClassRule
    public static final HeliosDeploymentResource DEPLOYMENT = new HeliosDeploymentResource(
            HeliosSoloDeployment.fromEnv().build());

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.create(DEPLOYMENT.client());

    @Test
    public void testDeployToSolo() throws Exception {
      final TemporaryJob job = temporaryJobs.job()
              .command(IDLE_COMMAND)
              .deploy();

      final Map<JobId, Job> jobs = DEPLOYMENT.client().jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 1, jobs.size());
      for (Job j : jobs.values()) {
        assertEquals("wrong job running", BUSYBOX, j.getImage());
      }

      job.undeploy();
    }
  }
}
