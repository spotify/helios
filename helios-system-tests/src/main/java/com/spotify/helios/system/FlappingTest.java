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

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FlappingTest extends SystemTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    // This job will kill the next container if it's running
    final Job killer = Job.newBuilder()
        .setName(testJobName + "killer")
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("sh", "-c", "trap 'exit 0' SIGINT SIGTERM; "
                                       + "while :; do nc -p 4711 -l; done"))
        .setPorts(ImmutableMap.of("listen", PortMapping.of(4711, externalPort)))
        .build();

    // This job will die if it can connect to the killer container above
    final Job flapper = Job.newBuilder()
        .setName(testJobName + "flapper")
        .setVersion(testJobVersion)
        .setImage(ALPINE)
        .setCommand(asList("sh", "-c", String.format(
            "while ! nc -vz %s %d; do sleep 1; done",
            DOCKER_HOST.address(), externalPort)))
        .build();

    // Start the kill container
    final JobId killerJobId = createJob(killer);
    deployJob(killerJobId, host);
    awaitTaskState(killerJobId, host, RUNNING);

    // Start the container we want to flap
    final JobId flapperJobId = createJob(flapper);
    deployJob(flapperJobId, host);
    awaitTaskState(flapperJobId, host, RUNNING);

    // Wait for the flapper container to restart enough times to be classified as flapping
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final JobStatus jobStatus = getOrNull(client.jobStatus(flapperJobId));
        final TaskStatus taskStatus = jobStatus.getTaskStatuses().get(host);
        if (taskStatus.getThrottled() == FLAPPING) {
          return true;
        }
        return null;
      }
    });

    // Verify that the job recovers after we undeploy the killer container
    stopJob(killerJobId, host);
    awaitJobThrottle(client, host, flapperJobId, NO, LONG_WAIT_SECONDS, SECONDS);
  }
}
