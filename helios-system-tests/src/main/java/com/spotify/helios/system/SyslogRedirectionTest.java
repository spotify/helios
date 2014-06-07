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

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static com.spotify.docker.client.DockerClient.LogsParameter.STDERR;
import static com.spotify.docker.client.DockerClient.LogsParameter.STDOUT;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

// TODO (dano): open source the syslog-redirector and enable this test
@Ignore("This needs the syslog-redirector")
public class SyslogRedirectionTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    // While this test doesn't specifically test that the output actually goes to syslog, it tests
    // just about every other part of it, and specifically, that the output doesn't get to
    // docker, and that the redirector executable exists and doesn't do anything terribly stupid.
    startDefaultMaster();
    startDefaultAgent(testHost(), "--syslog-redirect", "10.0.3.1:6514");
    awaitHostStatus(testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    final DockerClient dockerClient = new DefaultDockerClient(DOCKER_HOST.uri());

    final List<String> command = asList("sh", "-c", "echo should-be-redirected");

    // Create job
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, command,
                                  ImmutableMap.of("FOO", "4711",
                                                  "BAR", "deadbeef"));

    // deploy
    deployJob(jobId, testHost());

    final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

    final String log;
    try (LogStream logs = dockerClient.logs(taskStatus.getContainerId(), STDOUT, STDERR)) {
      log = logs.readFully();
    }

    // should be nothing in the docker output log, either error text or our message
    assertEquals("", log);
  }

}
