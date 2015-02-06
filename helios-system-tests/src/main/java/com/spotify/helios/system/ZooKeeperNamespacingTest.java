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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZooKeeperNamespacingTest extends SystemTestBase {
  private static final String NAMESPACE = "testing";

  @BeforeClass
  public static void setUpZkNamespace() {
    zooKeeperNamespace = NAMESPACE;
  }

  @Test
  public void test() throws Exception {
    startDefaultMaster("--zk-namespace", NAMESPACE);
    startDefaultAgent(testHost(), "--zk-namespace", NAMESPACE);
    awaitHostRegistered(testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);

    final CuratorFramework framework = CuratorFrameworkFactory.builder()
        .retryPolicy(zooKeeperRetryPolicy)
        .connectString(zk().connectString())
        .connectionTimeoutMs(30000)
        .sessionTimeoutMs(30000)
        .build();
    framework.start();
    final List<String> result = framework.getChildren().forPath("/");
    Collections.sort(result);
    assertEquals(2, result.size());
    assertEquals(NAMESPACE, result.get(0));
    assertEquals("zookeeper", result.get(1)); // we'll always find this
  }

  @Test
  public void testDeployment() throws Exception {
    startDefaultMaster("--zk-namespace", NAMESPACE);
    startDefaultAgent(testHost(), "--zk-namespace", NAMESPACE);

    final HeliosClient client = defaultClient();

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START, TEST_USER);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Undeploy the job
    final JobUndeployResponse undeployed = client.undeploy(jobId, testHost()).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final Deployment undeployedJob = client.deployment(testHost(), jobId).get();
    assertTrue(undeployedJob == null);

    // Wait for the task to disappear
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, client.deleteJob(jobId).get().getStatus());
  }
}
