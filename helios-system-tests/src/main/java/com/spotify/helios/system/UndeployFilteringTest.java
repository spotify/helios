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

import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.master.ZooKeeperMasterModel;
import com.spotify.helios.servicescommon.KafkaSender;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class UndeployFilteringTest extends SystemTestBase {

  private static final String TEST_HOST = "testhost";

  private CuratorFramework curator;
  private ZooKeeperClientProvider zkcp;
  private ZooKeeperMasterModel zkMasterModel;
  private AgentMain agent;
  private HeliosClient client;

  // Create a job
  private final Job job = Job.newBuilder()
      .setName(testJobName)
      .setVersion(testJobVersion)
      .setImage(BUSYBOX)
      .setCommand(IDLE_COMMAND)
      .build();

  @Before
  public void setUp() throws Exception {
    // make zookeeper interfaces
    curator = zk().curatorWithSuperAuth();

    zkcp = new ZooKeeperClientProvider(
        new DefaultZooKeeperClient(curator), ZooKeeperModelReporter.noop());

    final KafkaSender kafkaSender = mock(KafkaSender.class);

    zkMasterModel = new ZooKeeperMasterModel(zkcp, getClass().getName(), kafkaSender);
    startDefaultMaster();

    agent = startDefaultAgent(TEST_HOST);
    client = defaultClient();
    awaitHostRegistered(client, TEST_HOST, LONG_WAIT_SECONDS, SECONDS);
  }

  @Test
  public void testMaster() throws Exception {
    final JobId jobId = createAndAwaitJobRunning();

    // shut down the agent so it cannot remove the tombstone we make
    agent.stopAsync().awaitTerminated();

    // make sure things look correct before
    assertFalse(zkMasterModel.getJobs().isEmpty());
    assertEquals(START, zkMasterModel.getDeployment(TEST_HOST, jobId).getGoal());

    // undeploy job
    client.undeploy(jobId, TEST_HOST).get();

    // These used to be filtered away
    assertNull(zkMasterModel.getDeployment(TEST_HOST, jobId));
    assertTrue(zkMasterModel.getHostStatus(TEST_HOST).getJobs().isEmpty());
  }

  @Test
  public void testAgent() throws Exception {
    final JobId jobId = createAndAwaitJobRunning();

    final byte[] data1 = curator.getData().forPath(Paths.statusHostJob(TEST_HOST, jobId));
    assertNotNull(data1);
    final TaskStatus status = Json.read(data1, TaskStatus.class);
    assertNotNull(status);
    assertEquals(START, status.getGoal());
    assertEquals(RUNNING, status.getState());

    // stop so we can create and maintain the tombstone
    agent.stopAsync().awaitTerminated();

    // create tombstone
    client.undeploy(jobId, TEST_HOST).get();

    final byte[] data2 = curator.getData().forPath(Paths.statusHostJob(TEST_HOST, jobId));
    assertNotNull(data2);
    final TaskStatus status2 = Json.read(data2, TaskStatus.class);
    assertNotNull(status2);
    assertEquals(START, status2.getGoal());
    assertEquals(RUNNING, status2.getState());
  }

  private JobId createAndAwaitJobRunning() throws Exception {
    final CreateJobResponse jobby = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, jobby.getStatus());
    final JobId jobId = job.getId();
    final JobDeployResponse deployResponse = client.deploy(
        Deployment.of(jobId, START), TEST_HOST).get();
    assertEquals(JobDeployResponse.Status.OK, deployResponse.getStatus());
    awaitJobState(client, TEST_HOST, jobId, State.RUNNING, 30, TimeUnit.SECONDS);
    return jobId;
  }

}
