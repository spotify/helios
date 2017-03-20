/*-
 * -\-\-
 * Helios System Tests
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.system;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.lang.Integer.toHexString;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.helios.client.HeliosClient;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ReapingTest extends SystemTestBase {

  private final DockerClient docker;

  public ReapingTest() throws Exception {
    this.docker = getNewDockerClient();
  }

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
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // With LXC, killing a container results in exit code 0.
    // In docker 1.5 killing a container results in exit code 137, in previous versions it's -1.
    final String executionDriver = docker.info().executionDriver();
    final List<Integer> expectedExitCodes =
        (executionDriver != null && executionDriver.startsWith("lxc-"))
        ? Collections.singletonList(0) : asList(-1, 137);

    // Wait for the agent to kill the container
    final ContainerExit exit1 = docker.waitContainer(intruder1);
    assertThat(exit1.statusCode(), isIn(expectedExitCodes));

    // Start another container in the agent namespace
    startContainer(intruder2);

    // Wait for the agent to kill the second container as well
    final ContainerExit exit2 = docker.waitContainer(intruder2);
    assertThat(exit2.statusCode(), isIn(expectedExitCodes));
  }

  private String intruder(final String namespace) {
    return namespace + "-unwanted-" + toHexString(new SecureRandom().nextInt()) + "-" + testTag;
  }

  private void startContainer(final String name)
      throws DockerException, InterruptedException {
    docker.pull(BUSYBOX);
    final HostConfig hostConfig = HostConfig.builder().build();
    final ContainerConfig config = ContainerConfig.builder()
        .image(BUSYBOX)
        .cmd(IDLE_COMMAND)
        .hostConfig(hostConfig)
        .build();
    final ContainerCreation creation = docker.createContainer(config, name);
    final String containerId = creation.id();
    docker.startContainer(containerId);
  }
}
