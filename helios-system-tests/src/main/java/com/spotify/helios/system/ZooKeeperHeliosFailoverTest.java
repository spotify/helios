/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.spotify.helios.ZooKeeperClusterTestManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Before;
import org.junit.Test;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ZooKeeperHeliosFailoverTest extends SystemTestBase {

  private final Job FOO = Job.newBuilder()
      .setName(PREFIX + "foo")
      .setVersion(JOB_VERSION)
      .setImage("busybox")
      .setCommand(DO_NOTHING_COMMAND)
      .build();

  private final Job BAR = Job.newBuilder()
      .setName(PREFIX + "bar")
      .setVersion(JOB_VERSION)
      .setImage("busybox")
      .setCommand(DO_NOTHING_COMMAND)
      .build();

  private final ZooKeeperClusterTestManager zkc = new ZooKeeperClusterTestManager();

  private HeliosClient client;

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

  @Before
  public void setup() throws Exception {
    startDefaultMaster();
    startDefaultAgent(getTestHost());
    client = defaultClient();
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);
  }

  @Test
  public void verifyCanDeployWithOnePeerDead() throws Exception {
    deploy(FOO);
    zkc.stopPeer(0);
    undeploy(FOO.getId());
    deploy(BAR);
  }

  @Test
  public void verifyCanDeployWithOneNodeDeadAfterOneNodeDataLoss() throws Exception {
    deploy(FOO);
    zkc.stopPeer(0);
    zkc.resetPeer(0);
    zkc.startPeer(0);
    zkc.awaitUp(LONG_WAIT_MINUTES, MINUTES);
    zkc.stopPeer(1);
    undeploy(FOO.getId());
    deploy(BAR);
  }

  private void deploy(final Job job) throws Exception {
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    awaitJobState(client, getTestHost(), jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
  }

  private void undeploy(final JobId jobId) throws Exception {
    // Check job status can be queried
    final JobStatus jobStatus = client.jobStatus(jobId).get();
    assertEquals(RUNNING, jobStatus.getTaskStatuses().get(getTestHost()).getState());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, getTestHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Wait for the task to disappear
    awaitTaskGone(client, getTestHost(), jobId, LONG_WAIT_MINUTES, MINUTES);
  }

}
