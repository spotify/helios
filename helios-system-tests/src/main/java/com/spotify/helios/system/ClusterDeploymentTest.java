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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.ZooKeeperTestingClusterManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Optional.fromNullable;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClusterDeploymentTest extends SystemTestBase {

  public static final int HOSTS =
      Integer.valueOf(fromNullable(getenv("HELIOS_CLUSTER_DEPLOYMENT_TEST_HOSTS")).or("10"));

  private final Job job = Job.newBuilder()
      .setName("foo")
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
    client = defaultClient();
  }

  @Test
  public void verifyCanDeployOnSeveralHosts() throws Exception {
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final List<AgentMain> agents = Lists.newArrayList();

    for (int i = 0; i < HOSTS; i++) {
      final AgentMain agent =
          startDefaultAgent(host(i), "--no-http", "--no-metrics");
      agents.add(agent);
    }

    for (final AgentMain agent : agents) {
      agent.awaitRunning();
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitHostStatus(client, host(i), UP, LONG_WAIT_SECONDS, SECONDS);
    }

    for (int i = 0; i < HOSTS; i++) {
      deploy(job, host(i));
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitJobState(client, host(i), job.getId(), RUNNING, LONG_WAIT_SECONDS, SECONDS);
    }

    for (int i = 0; i < HOSTS; i++) {
      undeploy(job.getId(), host(i));
    }

    for (int i = 0; i < HOSTS; i++) {
      awaitTaskGone(client, host(i), job.getId(), LONG_WAIT_SECONDS, SECONDS);
    }
  }

  private String host(final int i) throws InterruptedException, ExecutionException,
                                          HeliosAuthException {
    return testHost() + i;
  }

  private void deploy(final Job job, final String host) throws Exception {
    Futures.addCallback(client.deploy(Deployment.of(job.getId(), START), host),
                        new FutureCallback<JobDeployResponse>() {
                          @Override
                          public void onSuccess(final JobDeployResponse result) {
                            assertEquals(JobDeployResponse.Status.OK, result.getStatus());
                          }

                          @Override
                          public void onFailure(final Throwable t) {
                            fail("deploy failed");
                          }
                        });
  }

  private void undeploy(final JobId jobId, final String host) throws Exception {

    Futures.addCallback(client.undeploy(jobId, host), new FutureCallback<JobUndeployResponse>() {
      @Override
      public void onSuccess(final JobUndeployResponse result) {
        assertEquals(JobUndeployResponse.Status.OK, result.getStatus());
      }

      @Override
      public void onFailure(final Throwable t) {
        fail("undeploy failed");
      }
    });
  }
}
