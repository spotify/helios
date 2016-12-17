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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;

public class BindVolumeTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    try (final DockerClient docker = getNewDockerClient()) {
      // Start Helios agent, configured to bind host /etc/hostname into container /mnt/hostname
      startDefaultMaster();
      startDefaultAgent(testHost(), "--bind", "/etc/hostname:/mnt/hostname:ro");
      awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

      // Figure out the host kernel version
      final String hostname = docker.info().name();

      // Run a job that cat's /mnt/hostname, which should be the host's name
      final List<String> command = ImmutableList.of("cat", "/mnt/hostname");
      final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, command);
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (LogStream logs = docker.logs(taskStatus.getContainerId(), stdout(), stderr())) {
        log = logs.readFully();
      }

      // the kernel version from the host should be in the log
      assertThat(log, containsString(hostname));
    }
  }

}
