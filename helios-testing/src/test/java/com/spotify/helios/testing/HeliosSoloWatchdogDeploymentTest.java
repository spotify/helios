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
import com.google.common.collect.Lists;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HeliosSoloWatchdogDeploymentTest {

  private static final String IMAGE_NAME = "busybox:latest";
  public static final List<String> IDLE_COMMAND = ImmutableList.of(
      "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");
  private static final HeliosSoloDeployment SOLO_DEPLOYMENT =
      HeliosSoloDeployment.fromEnv().build();

  // Don't use @ClassRule so that we don't invoke after()
  private static final HeliosDeploymentResource SOLO_RESOURCE =
      new HeliosDeploymentResource(SOLO_DEPLOYMENT);

  // Don't use @Rule so that we don't invoke after()
  private static final TemporaryJobs TEMPORARY_JOBS = TemporaryJobs.builder()
      .jobPrefix("HeliosSoloDeploymentTest2")
      .client(SOLO_RESOURCE.client())
      .build();

  @Test
  public void testDeployToSolo() throws Throwable {
    SOLO_RESOURCE.before();
    TEMPORARY_JOBS.before();

    // while ".*" is the default in the local testing profile, explicitly specify it here
    // to avoid any extraneous environment variables for HELIOS_HOST_FILTER that might be set
    // on the build agent executing this test from interfering with the behavior we want here.
    // Since we are deploying on a self-contained helios-solo container, any
    // HELIOS_HOST_FILTER value set for other tests will never match the agent hostname
    // inside helios-solo.
    final String hostFilter = ".*";

    // Deploy two jobs via Helios solo
    final TemporaryJob job1 =
        TEMPORARY_JOBS.job().hostFilter(hostFilter).command(IDLE_COMMAND).deploy();
    final TemporaryJob job2 =
        TEMPORARY_JOBS.job().hostFilter(hostFilter).command(IDLE_COMMAND).deploy();

    // Make a list of container IDs related to this test, ie the solo container with the two above
    // jobs
    final List<String> containerIds = Lists.newArrayList(SOLO_DEPLOYMENT.heliosContainerId());
    for (Map.Entry<String, TaskStatus> s : job1.statuses().entrySet()) {
      containerIds.add(s.getValue().getContainerId());
    }
    for (Map.Entry<String, TaskStatus> s : job2.statuses().entrySet()) {
      containerIds.add(s.getValue().getContainerId());
    }

    final Map<JobId, Job> jobs = SOLO_RESOURCE.client().jobs().get(15, SECONDS);

    assertEquals("wrong number of jobs running", 2, jobs.size());
    for (final Job j : jobs.values()) {
      assertEquals("wrong job running", IMAGE_NAME, j.getImage());
    }

    assertTrue(SOLO_DEPLOYMENT.disconnectFromWatchdog());

    // Wait until the watchdog.py process kills the two jobs above and helios-solo as well.
    // This interval needs to be slightly longer than the TIMEOUT in watchdog.py.
    Thread.sleep(TimeUnit.SECONDS.toMillis(65));

    // Make a list of currently running container IDs and
    final DefaultDockerClient dockerClient = DefaultDockerClient.fromEnv().build();
    final List<String> postContainerIds = Lists.newArrayList();
    for (final Container c : dockerClient.listContainers()) {
      postContainerIds.add(c.id());
    }

    // Check that containerIds are not in postContainerIds
    for (final String id : containerIds) {
      assertThat(postContainerIds, not(hasItem(id)));
    }
  }
}
