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
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import org.junit.Test;

public class DnsServerTest extends SystemTestBase {

  @Test
  public void testDnsParam() throws Exception {
    final String server1 = "127.0.0.1";
    final String server2 = "127.0.0.2";
    startDefaultMaster();
    startDefaultAgent(testHost(), "--dns", server1, "--dns", server2);
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX,
        asList("cat", "/etc/resolv.conf"));

    deployJob(jobId, testHost());

    final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);
    try (final DockerClient dockerClient = getNewDockerClient()) {
      final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), stdout(), stderr());
      final String log = logs.readFully();

      assertThat(log, containsString(server1));
      assertThat(log, containsString(server2));
    }
  }

  @Test
  public void testNoDnsParam() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX,
        asList("cat", "/etc/resolv.conf"));

    deployJob(jobId, testHost());

    final TaskStatus taskStatus = awaitTaskState(jobId, testHost(), EXITED);
    try (final DockerClient dockerClient = getNewDockerClient()) {
      final LogStream logs = dockerClient.logs(taskStatus.getContainerId(), stdout(), stderr());
      final String log = logs.readFully();

      // Verify that a nameserver is set even if we don't specify the --dns param
      assertThat(log, containsString("nameserver"));
    }
  }

}
