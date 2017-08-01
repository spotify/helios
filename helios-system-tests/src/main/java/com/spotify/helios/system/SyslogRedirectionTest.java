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

import static com.spotify.docker.client.DockerClient.LogsParam.stderr;
import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerRequestException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SyslogRedirectionTest extends SystemTestBase {

  private static final Pattern DEFAULT_GATEWAY_PATTERN =
      Pattern.compile("^default via (?<gateway>[0-9.]+)");

  private final String testImage = testTag + "_helios-syslog-test";

  private String syslogHost;

  @Before
  public void setup() throws Exception {
    try (final DockerClient docker = getNewDockerClient()) {
      // Build an image with an ENTRYPOINT and CMD prespecified
      final String dockerDirectory = Resources.getResource("syslog-test-image").getPath();
      docker.build(Paths.get(dockerDirectory), testImage);

      // Figure out the host IP from the container's point of view (needed for syslog)
      final ContainerConfig config = ContainerConfig.builder()
          .image(BUSYBOX)
          .cmd(asList("ip", "route", "show"))
          .build();
      final ContainerCreation creation = docker.createContainer(config);
      final String containerId = creation.id();
      docker.startContainer(containerId);

      // Wait for the container to exit.
      // If we don't wait, docker.logs() might return an epmty string because the container
      // cmd hasn't run yet.
      docker.waitContainer(containerId);

      final String log;
      try (LogStream logs = docker.logs(containerId, stdout(), stderr())) {
        log = logs.readFully();
      }

      final Matcher m = DEFAULT_GATEWAY_PATTERN.matcher(log);
      if (m.find()) {
        syslogHost = m.group("gateway");
      } else {
        fail("couldn't determine the host address from '" + log + "'");
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    try (final DockerClient docker = getNewDockerClient()) {
      try {
        docker.removeImage(testImage, true, false);
      } catch (DockerException e) {
        // oh well, we tried
      }
    }
  }

  @Test
  public void test() throws Exception {
    final String syslogOutput = "should-be-redirected";

    try (final DockerClient docker = getNewDockerClient()) {
      // Start a container that will be our "syslog" endpoint (just run netcat and print whatever
      // we receive).
      final String port = "4711";
      final String expose = port + "/udp";

      docker.pull(ALPINE);

      final HostConfig hostConfig = HostConfig.builder()
          .publishAllPorts(true)
          .build();
      final ContainerConfig config = ContainerConfig.builder()
          .image(ALPINE) // includes spotify/busybox:latest with netcat with udp support
          .cmd(asList("nc", "-p", port, "-l", "-u"))
          .exposedPorts(ImmutableSet.of(expose))
          .hostConfig(hostConfig)
          .build();
      final ContainerCreation creation = docker.createContainer(config, testTag + "_syslog");
      final String syslogContainerId = creation.id();
      docker.startContainer(syslogContainerId);

      final ContainerInfo containerInfo = docker.inspectContainer(syslogContainerId);
      assertThat(containerInfo.state().running(), equalTo(true));

      final String syslogEndpoint =
          syslogHost + ":" + containerInfo.networkSettings().ports().get(expose).get(0).hostPort();

      // Run a Helios job that logs to syslog.
      startDefaultMaster();
      startDefaultAgent(testHost(), "--syslog-redirect", syslogEndpoint);
      awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

      final List<String> command = Lists.newArrayList();
      final JobId jobId = createJob(testJobName, testJobVersion, testImage, command,
          ImmutableMap.of("SYSLOG_REDIRECTOR", "/syslog-redirector"));
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      {
        // Verify the log for the task container
        LogStream logs = null;
        try {
          logs = docker.logs(taskStatus.getContainerId(), stdout(), stderr());
          final String log = logs.readFully();

          // for old docker versions should be nothing in the docker output log, either error text
          // or our message
          assertEquals("", log);
        } catch (DockerRequestException e) {
          // for new docker versions, trying to read logs should throw an error but the syslog
          // option should be set
          final String logType = docker.inspectContainer(taskStatus.getContainerId())
              .hostConfig().logConfig().logType();
          assertEquals("syslog", logType);
        } finally {
          if (logs != null) {
            logs.close();
          }
        }
      }

      // Verify the log for the syslog container
      {
        final String log;
        try (LogStream logs = docker.logs(syslogContainerId, stdout(), stderr())) {
          log = logs.readFully();
        }

        // the output message from the command should appear in the syslog container
        assertThat(log, containsString(syslogOutput));
      }
    }
  }

}
