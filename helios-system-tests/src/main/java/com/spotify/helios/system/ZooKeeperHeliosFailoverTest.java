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

import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperTestingClusterManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.KeeperException.NodeExistsException;
import static org.junit.Assert.assertEquals;

public class ZooKeeperHeliosFailoverTest extends SystemTestBase {

  private final Job fooJob = Job.newBuilder()
      .setName(testTag + "foo")
      .setVersion(testJobVersion)
      .setImage(BUSYBOX)
      .setCommand(IDLE_COMMAND)
      .build();

  private final Job barJob = Job.newBuilder()
      .setName(testTag + "bar")
      .setVersion(testJobVersion)
      .setImage(BUSYBOX)
      .setCommand(IDLE_COMMAND)
      .build();

  private final ZooKeeperTestingClusterManager zkc = new ZooKeeperTestingClusterManager();

  private HeliosClient client;

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

  @Before
  public void setup() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    client = defaultClient();
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);
  }

  @Test
  public void verifyCanDeployWithOnePeerDead() throws Exception {
    deploy(fooJob);
    zkc.stopPeer(0);
    undeploy(fooJob.getId());
    deploy(barJob);
  }

  @Test
  public void verifyCanDeployWithOneNodeDeadAfterOneNodeDataLoss() throws Exception {
    // First deploy a job
    deploy(fooJob);

    // Create a node that we know is written after the job
    try {
      zkc.curatorWithSuperAuth().create().forPath("/barrier");
    } catch (NodeExistsException ignore) {
    }

    // Wipe one zk peer
    zkc.stopPeer(0);
    zkc.resetPeer(0);
    zkc.startPeer(0);

    // Wait for the zk peer to recover
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return zkc.peerCurator(0).checkExists().forPath("/barrier");
      }
    });

    // Then take down another peer
    zkc.stopPeer(1);

    // Now verify that we can still undeploy and deploy jobs
    undeploy(fooJob.getId());
    deploy(barJob);
  }

  private void deploy(final Job job) throws Exception {
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
  }

  private void undeploy(final JobId jobId) throws Exception {
    // Check job status can be queried
    final JobStatus jobStatus = client.jobStatus(jobId).get();
    assertEquals(RUNNING, jobStatus.getTaskStatuses().get(testHost()).getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, testHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Wait for the task to disappear
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);
  }

}
