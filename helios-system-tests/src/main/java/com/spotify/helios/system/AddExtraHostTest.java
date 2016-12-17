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

/** Test of --add-host in the agent. */
public class AddExtraHostTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    try (final DockerClient docker = getNewDockerClient()) {
      // Start Helios agent, configured to bind host /etc/hostname into container /mnt/hostname
      startDefaultMaster();
      startDefaultAgent(testHost(), "--add-host", "secrethost:169.254.169.254");
      awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

      // a job that cat's /etc/hosts
      final List<String> command = ImmutableList.of("cat", "/etc/hosts");
      final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, command);
      deployJob(jobId, testHost());

      final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);

      final String log;
      try (LogStream logs = docker.logs(taskStatus.getContainerId(), stdout(), stderr())) {
        log = logs.readFully();

        assertThat(log, containsString("169.254.169.254\tsecrethost"));
      }
    }
  }
}
