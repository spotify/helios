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

package com.spotify.helios.system;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class JobExpirationTest extends SystemTestBase {

  private final DockerClient docker = new DefaultDockerClient(DOCKER_HOST.uri());

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost());
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND,
        DateTime.now().plusSeconds(10).toDate());

    deployJob(jobId, testHost());

    // Make sure the job runs
    TaskStatus taskStatus = awaitJobState(
        client,
        testHost(),
        jobId,
        RUNNING,
        WAIT_TIMEOUT_SECONDS,
        SECONDS);

    // Then make sure it expires
    Polling.await(3, MINUTES, new Callable<JobId>() {
      @Override
      public JobId call() throws Exception {
        if (client.jobs().get().containsKey(jobId)) {
          return null; // job still exists, return null to continue polling
        } else {
          return jobId; // job no longer exists, return non-null to exit polling
        }
      }
    });

    // Wait for the agent to kill the container
    final ContainerExit exit = docker.waitContainer(taskStatus.getContainerId());
    assertThat(exit.statusCode(), is(-1));
  }
}
