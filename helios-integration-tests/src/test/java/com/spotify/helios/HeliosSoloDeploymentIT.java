/*
 * Copyright (c) 2016 Spotify AB.
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


package com.spotify.helios;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.testing.HeliosDeploymentResource;
import com.spotify.helios.testing.HeliosSoloDeployment;
import com.spotify.helios.testing.TemporaryJobs;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;

public class HeliosSoloDeploymentIT {

  public static final String IMAGE_NAME = "onescience/alpine:latest";

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloDeploymentIT.class);

  @ClassRule
  public static final HeliosDeploymentResource DEPLOYMENT = new HeliosDeploymentResource(
      HeliosSoloDeployment.fromEnv()
          .heliosSoloImage(Utils.soloImage())
          .checkForNewImages(false)
          .removeHeliosSoloOnExit(false)
          .build());

  @Rule
  public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
      .jobPrefix("HeliosSoloDeploymentTest")
      .client(DEPLOYMENT.client())
      .build();

  @Test
  public void testDeployToSolo() throws Exception {
    temporaryJobs.job()
        // while ".*" is the default in the local testing profile, explicitly specify it here
        // to avoid any extraneous environment variables for HELIOS_HOST_FILTER that might be set
        // on the build agent executing this test from interfering with the behavior we want here.
        // Since we are deploying on a self-contained helios-solo container, any
        // HELIOS_HOST_FILTER value set for other tests will never match the agent hostname
        // inside helios-solo.
        .hostFilter(".*")
        .command(asList("sh", "-c", "nc -l -v -p 4711 -e true"))
        .image(IMAGE_NAME)
        .port("netcat", 4711)
        .registration("foobar", "tcp", "netcat")
        .deploy();

    final Map<JobId, Job> jobs = DEPLOYMENT.client().jobs().get(15, SECONDS);
    log.info("{} jobs deployed on helios-solo", jobs.size());
    assertEquals("wrong number of jobs running", 1, jobs.size());

    for (final Job j : jobs.values()) {
      log.info("job on helios-solo: {}", j);
      assertEquals("wrong job running", IMAGE_NAME, j.getImage());
    }
  }
}

