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
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Info;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Resources;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

public class ResourcesTest extends SystemTestBase {

  private HeliosClient client;
  private Job job;

  private static final Long MEMORY = 10485760L;
  private static final Long MEMORY_SWAP = 10485763L;
  private static final Long CPU_SHARES = 512L;
  private static final String CPUSET_CPUS = "0-1";

  @Before
  public void setup() throws Exception {
    startDefaultMaster();

    client = defaultClient();
    startDefaultAgent(testHost());

    job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setResources(new Resources(MEMORY, MEMORY_SWAP, CPU_SHARES, CPUSET_CPUS))
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
  }

  @Test
  public void testClient() throws Exception {
    // Doesn't work on CircleCI because their lxc-driver can't set cpus
    // See output of `docker run --cpuset-cpus 0-1 spotify/busybox:latest true`
    assumeThat(getenv("CIRCLECI"), isEmptyOrNullString());

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
    assertJobEquals(job, taskStatus.getJob());

    try (final DockerClient docker = getNewDockerClient()) {

      final HostConfig hostConfig =
          docker.inspectContainer(taskStatus.getContainerId()).hostConfig();

      assertEquals(CPU_SHARES, hostConfig.cpuShares());
      assertEquals(CPUSET_CPUS, hostConfig.cpusetCpus());

      final Info info = docker.info();
      final Iterable<String> split = Splitter.on(".").split(docker.version().apiVersion());
      //noinspection ConstantConditions
      final int major = Integer.parseInt(Iterables.get(split, 0, "0"));
      //noinspection ConstantConditions
      final int minor = Integer.parseInt(Iterables.get(split, 1, "0"));

      // TODO (dxia) This doesn't work on docker < 1.7 ie docker API < 1.19 for some reason.
      if (major >= 1 && minor >= 19) {
        if (info.memoryLimit()) {
          assertEquals(MEMORY, hostConfig.memory());
        }
        if (info.swapLimit()) {
          assertEquals(MEMORY_SWAP, hostConfig.memorySwap());
        }
      }
    }
  }
}
