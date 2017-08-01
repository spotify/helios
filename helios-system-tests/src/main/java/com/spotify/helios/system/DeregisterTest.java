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
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarUtil;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;

public class DeregisterTest extends SystemTestBase {

  @Rule public final TemporaryPorts ports = TemporaryPorts.create();

  @Test
  public void testDeregisterHostThatDoesntExist() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    final HeliosClient client = defaultClient();

    final HostDeregisterResponse deregisterResponse = client.deregisterHost(host).get();
    assertEquals(HostDeregisterResponse.Status.NOT_FOUND, deregisterResponse.getStatus());
  }

  @Test
  public void testDeregister() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    final AgentMain agent = startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(4711),
            "bar", PortMapping.of(4712, ports.localPort("bar"))))
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, host, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, host).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    awaitJobState(client, host, jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    // Kill off agent
    agent.stopAsync().awaitTerminated();

    // Deregister agent
    final HostDeregisterResponse deregisterResponse = client.deregisterHost(host).get();
    assertEquals(HostDeregisterResponse.Status.OK, deregisterResponse.getStatus());

    // Verify that it's possible to remove the job
    final JobDeleteResponse deleteResponse = client.deleteJob(jobId).get();
    assertEquals(JobDeleteResponse.Status.OK, deleteResponse.getStatus());
  }

  // Verify that we can deregister a host there are jobs deployed to it, for which there's no
  // corresponding status information. For example, if a job was deployed to the host after is went
  // down.
  @Test
  public void testDeregisterJobDeployedWithoutStatus() throws Exception {
    startDefaultMaster();
    final String host = testHost();

    final HeliosClient client = defaultClient();
    final DefaultZooKeeperClient zkClient =
        new DefaultZooKeeperClient(zk().curatorWithSuperAuth());

    final String idPath = Paths.configHostId(host);
    ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, host, UUID.randomUUID().toString());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(4711),
            "bar", PortMapping.of(4712, ports.localPort("bar"))))
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, host).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Deregister agent
    final HostDeregisterResponse deregisterResponse = client.deregisterHost(host).get();
    assertEquals(HostDeregisterResponse.Status.OK, deregisterResponse.getStatus());

    // Verify that it's possible to remove the job
    final JobDeleteResponse deleteResponse = client.deleteJob(jobId).get();
    assertEquals(JobDeleteResponse.Status.OK, deleteResponse.getStatus());
  }

  @Test
  public void testRegistrationResolution() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    final AgentMain agent = startDefaultAgent(host, "--labels", "num=1");

    final HeliosClient client = defaultClient();

    // Wait for agent to come up
    awaitHostRegistered(client, host, LONG_WAIT_SECONDS, SECONDS);
    // Wait for agent to be UP and report HostInfo
    awaitHostStatusWithHostInfo(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    // Kill off agent
    agent.stopAsync().awaitTerminated();
    awaitHostStatus(client, host, DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Start a new agent with the same hostname but have it generate a different ID
    resetAgentStateDir();

    startDefaultAgent(host, "--zk-registration-ttl", "0", "--labels", "num=2");

    // Check that the new host is registered
    awaitHostRegistered(client, host, LONG_WAIT_SECONDS, SECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void testRegistrationResolutionTtlNotExpired() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    final AgentMain agent = startDefaultAgent(host);

    final HeliosClient client = defaultClient();

    // Wait for agent to come up
    awaitHostRegistered(client, host, LONG_WAIT_SECONDS, SECONDS);
    // Wait for agent to be UP and report HostInfo
    awaitHostStatusWithHostInfo(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    // Kill off agent
    agent.stopAsync().awaitTerminated();
    awaitHostStatus(client, host, DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Start a new agent with the same hostname but have it generate a different ID
    resetAgentStateDir();

    // Set TTL to a large number so new agent will not deregister previous one.
    // This might throw IllegalStateException as this agent will fail to start since it can't
    // register. This exception sometimes occurs and sometimes doesn't. We ignore that and
    // instead check for the TimeoutException while polling for it being UP.
    try {
      startDefaultAgent(host, "--zk-registration-ttl", "9999");
    } catch (IllegalStateException ignored) {
      // ignored
    }
    awaitHostStatus(client, host, UP, 10, SECONDS);
  }

  @Test
  public void testJobsArePreservedWhenReregistering() throws Exception {
    startDefaultMaster();
    final String host = testHost();
    final AgentMain agent = startDefaultAgent(host, "--labels", "num=1");
    final HeliosClient client = defaultClient();
    awaitHostStatus(client, host, UP, LONG_WAIT_SECONDS, SECONDS);

    // Deploy a job and wait for it to be running
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId, host);
    awaitJobState(client, host, jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    // Kill off agent
    agent.stopAsync().awaitTerminated();
    awaitHostStatus(client, host, DOWN, LONG_WAIT_SECONDS, SECONDS);

    // Start a new agent with the same hostname but have it generate a different ID
    resetAgentStateDir();

    startDefaultAgent(host, "--zk-registration-ttl", "0", "--labels", "num=2");

    // Check that the new host is registered
    awaitHostRegistered(client, host, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatusWithLabels(client, host, UP, ImmutableMap.of("num", "2"));

    // Check that the job we previously deployed is preserved
    awaitJobState(client, host, jobId, RUNNING, WAIT_TIMEOUT_SECONDS, SECONDS);
  }
}
