/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Test;

import java.security.SecureRandom;
import java.util.List;

public class NamespaceTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final String id = "test-" + Integer.toHexString(new SecureRandom().nextInt());
    final String namespace = "helios-" + id;

    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost(), "--id=" + id);

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    try (final DockerClient docker = getNewDockerClient()) {
      final List<Container> containers = docker.listContainers();
      Container jobContainer = null;
      for (final Container container : containers) {
        for (final String name : container.names()) {
          if (name.startsWith("/" + namespace)) {
            jobContainer = container;
          }
        }
      }

      assertNotNull(jobContainer);
    }
  }
}
