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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import java.util.List;
import org.junit.Test;

public class ContainerHostNameTest extends SystemTestBase {

  @Test
  public void testValidHostname() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    try (final DockerClient dockerClient = getNewDockerClient()) {

      final List<String> command = asList("hostname", "-f");

      // Create job
      final JobId jobId = createJob(Job.newBuilder()
          .setName(testJobName)
          .setVersion(testJobVersion)
          .setImage(BUSYBOX)
          .setHostname(testHost())
          .setCommand(command)
          .build());

      // deploy
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (final LogStream logs = dockerClient.logs(
          taskStatus.getContainerId(), stdout(), stderr())) {
        log = logs.readFully();
      }

      assertThat(log, containsString(testHost()));
    }
  }
}
