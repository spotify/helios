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
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.helios.client.HeliosClient;

import org.junit.Test;

import java.security.SecureRandom;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.Integer.toHexString;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ReapingTest extends SystemTestBase {

  private final DockerClient docker = new DefaultDockerClient(DOCKER_HOST.uri());

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final String id = "test-" + toHexString(new SecureRandom().nextInt());
    final String namespace = "helios-" + id;
    final String intruder1 = intruder(namespace);
    final String intruder2 = intruder(namespace);

    // Start a container in the agent namespace
    startContainer(intruder1);

    // Start agent
    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost(), "--id=" + id);
    awaitHostRegistered(client, testHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    int expectedExitCode = -1;
    if (docker.info().executionDriver().startsWith("lxc-")) {
      // with LXC, killing a container results in exit code 0
      expectedExitCode = 0;
    }

    // Wait for the agent to kill the container
    final ContainerExit exit1 = docker.waitContainer(intruder1);
    assertThat(exit1.statusCode(), is(expectedExitCode));

    // Start another container in the agent namespace
    startContainer(intruder2);

    // Wait for the agent to kill the second container as well
    final ContainerExit exit2 = docker.waitContainer(intruder2);
    assertThat(exit2.statusCode(), is(expectedExitCode));
  }

  private String intruder(final String namespace) {
    return namespace + "-unwanted-" + toHexString(new SecureRandom().nextInt()) + "-" + testTag;
  }

  private void startContainer(final String name)
      throws DockerException, InterruptedException {
    docker.pull(BUSYBOX);
    final ContainerConfig config = ContainerConfig.builder()
        .image(BUSYBOX)
        .cmd(IDLE_COMMAND)
        .build();
    final HostConfig hostConfig = HostConfig.builder().build();
    final ContainerCreation creation = docker.createContainer(config, name);
    final String containerId = creation.id();
    docker.startContainer(containerId, hostConfig);
  }
}
