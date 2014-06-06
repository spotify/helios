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

import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.servicescommon.coordination.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ZooKeeperRestoreTest extends SystemTestBase {

  private final Job fooJob = Job.newBuilder()
      .setName(testJobName)
      .setVersion(testJobVersion)
      .setImage(BUSYBOX)
      .setCommand(IDLE_COMMAND)
      .build();

  private final ZooKeeperStandaloneServerManager zkc = new ZooKeeperStandaloneServerManager();

  private HeliosClient client;
  private Path backupDir;

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

  @Before
  public void setup() throws Exception {
    backupDir = Files.createTempDirectory("helios-zk-updating-persistent-dir-test-backup-");

    startDefaultMaster();
    client = defaultClient();

    final CreateJobResponse created = client.createJob(fooJob).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(backupDir.toFile());
  }

  @Test
  public void verifyAgentReRegistersAfterRestore() throws Exception {
    // Back up zk
    zkc.backup(backupDir);

    // Start agent
    startDefaultAgent(getTestHost());
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Restore zk, erasing task state
    zkc.stop();
    zkc.restore(backupDir);
    zkc.start();

    // Wait for agent to reregister
    awaitHostRegistered(client, getTestHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);
  }

  @Test
  public void verifyAgentPushesTaskStateAfterRestore() throws Exception {

    // Start agent once to have it register
    final AgentMain agent1 = startDefaultAgent(getTestHost());
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);
    agent1.stopAsync().awaitTerminated();

    // Deploy job
    final Deployment deployment = Deployment.of(fooJob.getId(), START);
    final JobDeployResponse deployed = client.deploy(deployment, getTestHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Back up zk
    zkc.backup(backupDir);

    // Start agent
    startDefaultAgent(getTestHost());
    awaitHostStatus(client, getTestHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Wait for agent to indicate that job is running
    awaitJobState(client, getTestHost(), fooJob.getId(), RUNNING, LONG_WAIT_MINUTES, MINUTES);

    // Restore zk, erasing task state
    zkc.stop();
    zkc.restore(backupDir);
    zkc.start();

    // Wait for agent to again indicate that job is running
    awaitJobState(client, getTestHost(), fooJob.getId(), RUNNING, LONG_WAIT_MINUTES, MINUTES);

    // Remove task status
    zkc.curator().delete().forPath(Paths.statusHostJob(getTestHost(), fooJob.getId()));
  }
}
