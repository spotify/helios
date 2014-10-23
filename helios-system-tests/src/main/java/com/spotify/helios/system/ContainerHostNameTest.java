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

import com.google.common.base.Splitter;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.List;

import static com.spotify.docker.client.DockerClient.LogsParameter.STDERR;
import static com.spotify.docker.client.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;

public class ContainerHostNameTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri())) {

      final List<String> command = asList("hostname", "-f");

      // Create job
      final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, command);

      // deploy
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
        log = logs.readFully();
      }

      assertThat(log, containsString(testHost()));
    }
  }

  @Test
  public void testLength() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    try (final DefaultDockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri())) {
      final List<String> command = asList("hostname", "-f");

      // make something absurdly long
      final String jobName = testJobName
          + "01234567890123456789012345678901234567890123456789012345678901234567890";

      // Create job
      final JobId jobId = createJob(jobName, testJobVersion, BUSYBOX, command);

      // deploy
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
        log = logs.readFully();
      }

      final List<String> hostnameParts = Splitter.on(".").splitToList(log.trim());
      final int firstPartLen = hostnameParts.get(0).length();
      assertTrue(format("first part of host name should be <= 32 long, was %s", firstPartLen),
          firstPartLen <= 32);
    }
  }
}
