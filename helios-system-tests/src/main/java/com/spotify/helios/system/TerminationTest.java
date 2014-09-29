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
import com.spotify.docker.client.LogStream;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import static com.spotify.docker.client.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class TerminationTest extends SystemTestBase {

  @Test
  public void testNoIntOnExit() throws Exception {
    startDefaultMaster();

    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_MINUTES, MINUTES);

    // Note: signal 2 is SIGINT
    final Job jobToInterrupt = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("/bin/sh", "-c", "trap handle 2; handle() { echo int; exit 0; }; "
                                            + "while true; do sleep 1; done"))
        .build();

    final JobId jobId = createJob(jobToInterrupt);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, RUNNING);

    client.setGoal(new Deployment(jobId, Goal.STOP), host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, STOPPED);

    final String log;
    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri());
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT)) {
      log = logs.readFully();
    }

    // No message expected, since SIGINT should not be sent
    assertEquals("", log);
  }

  @Test
  public void testTermOnExit() throws Exception {
    startDefaultMaster();

    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_MINUTES, MINUTES);

    // Note: signal 15 is SIGTERM
    final Job jobToInterrupt = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("/bin/sh", "-c", "trap handle 15; handle() { echo term; exit 0; }; "
                                            + "while true; do sleep 1; done"))
        .build();

    final JobId jobId = createJob(jobToInterrupt);
    deployJob(jobId, host);
    awaitTaskState(jobId, host, RUNNING);

    client.setGoal(new Deployment(jobId, Goal.STOP), host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, STOPPED);

    final String log;
    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri());
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT)) {
      log = logs.readFully();
    }

    // Message expected, because the SIGTERM handler in the script should have run
    assertEquals("term\n", log);
  }

}
