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

import static com.spotify.docker.client.DockerClient.LogsParam.stdout;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import org.junit.Before;
import org.junit.Test;

public class TerminationTest extends SystemTestBase {

  @Before
  public void setup() throws Exception {
    // LXC has a bug where the TERM signal isn't sent to containers, so we can only run this test
    // if docker runs with the native driver.
    // See: https://github.com/docker/docker/issues/2436
    final DockerClient dockerClient = getNewDockerClient();
    assumeThat(dockerClient.info().executionDriver(), startsWith("native"));
  }

  @Test
  public void testNoIntOnExit() throws Exception {
    startDefaultMaster();

    final String host = testHost();
    startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

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

    client.setGoal(new Deployment(jobId, Goal.STOP, Deployment.EMTPY_DEPLOYER_USER,
        Deployment.EMPTY_DEPLOYER_MASTER,
        Deployment.EMPTY_DEPLOYMENT_GROUP_NAME), host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, STOPPED);

    final String log;
    try (final DockerClient dockerClient = getNewDockerClient();
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), stdout())) {
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

    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

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

    client.setGoal(new Deployment(jobId, Goal.STOP, Deployment.EMTPY_DEPLOYER_USER,
        Deployment.EMPTY_DEPLOYER_MASTER,
        Deployment.EMPTY_DEPLOYMENT_GROUP_NAME), host);

    final TaskStatus taskStatus = awaitTaskState(jobId, host, STOPPED);

    final String log;
    try (final DockerClient dockerClient = getNewDockerClient();
         LogStream logs = dockerClient.logs(taskStatus.getContainerId(), stdout())) {
      log = logs.readFully();
    }

    // Message expected, because the SIGTERM handler in the script should have run
    assertEquals("term\n", log);
  }

}
