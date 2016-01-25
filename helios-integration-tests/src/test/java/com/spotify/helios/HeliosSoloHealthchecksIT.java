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

package com.spotify.helios;

import com.spotify.helios.testing.HeliosDeploymentResource;
import com.spotify.helios.testing.HeliosSoloDeployment;
import com.spotify.helios.testing.TemporaryJobBuilder;
import com.spotify.helios.testing.TemporaryJobs;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests that jobs with http and tcp healthchecks can be successfully deployed to helios-solo.
 */
public class HeliosSoloHealthchecksIT {

  @Rule
  public HeliosDeploymentResource solo = new HeliosDeploymentResource(
      HeliosSoloDeployment.fromEnv()
          .heliosSoloImage(Utils.soloImage())
          .checkForNewImages(false)
          .removeHeliosSoloOnExit(false)
          .build()
  );

  @Rule
  public TemporaryJobs jobs = TemporaryJobs.builder()
      .client(solo.client())
      .deployTimeoutMillis(TimeUnit.MINUTES.toMillis(1))
      .build();

  private final TemporaryJobBuilder nginx = jobs.job()
      .image("nginx:1.9.9")
      .port("http", 80);

  @Test
  public void testHttpHealthcheck() {
    nginx.httpHealthCheck("http", "/")
        .deploy();
  }

  @Test
  public void testTcpHealthcheck() {
    nginx.tcpHealthCheck("http")
        .deploy();
  }
}
