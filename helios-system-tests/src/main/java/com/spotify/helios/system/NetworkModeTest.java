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

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import org.junit.Before;
import org.junit.Test;

public class NetworkModeTest extends SystemTestBase {

  private HeliosClient client;
  private Job job;

  private static final String NETWORK_MODE = "host";

  @Before
  public void setup() throws Exception {
    startDefaultMaster();

    client = defaultClient();
    startDefaultAgent(testHost());

    job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setNetworkMode("host")
        .setCommand(IDLE_COMMAND)
        .build();
  }

  @Test
  public void test() throws Exception {
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final JobId jobId = job.getId();
    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus taskStatus = awaitJobState(
        client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    try (final DockerClient docker = getNewDockerClient()) {
      final HostConfig hostConfig =
          docker.inspectContainer(taskStatus.getContainerId()).hostConfig();
      assertEquals(NETWORK_MODE, hostConfig.networkMode());
    }
  }
}
