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

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlappingTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    final Job flapper = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("nc", "-p", "4711", "-l"))
        .addPort("poke", PortMapping.of(4711))
        .build();

    final JobId jobId = createJob(flapper);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, RUNNING);

    // Poke the container to make it exit until it's classified as flapping
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final JobStatus jobStatus = getOrNull(client.jobStatus(jobId));
        final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(host);
        if (taskStatus.getThrottled() == FLAPPING) {
          return true;
        }
        final PortMapping port = taskStatus.getPorts().get("poke");
        assert port.getExternalPort() != null;
        poke(port.getExternalPort());
        return null;
      }
    });

    // Verify that the job recovers after we stop poking
    awaitJobThrottle(client, host, jobId, NO, LONG_WAIT_SECONDS, SECONDS);
  }

  private boolean poke(final int port) {
    try (Socket ignored = new Socket(DOCKER_HOST.address(), port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
