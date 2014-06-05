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

package com.spotify.helios;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.JobNotDeployedException;
import com.spotify.helios.master.JobStillDeployedException;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.master.ZooKeeperMasterModel;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperMasterModelIntegrationTest {

  private static final String IMAGE = "IMAGE";
  private static final String COMMAND = "COMMAND";
  private static final String JOB_NAME = "JOB_NAME";
  private static final String HOST = "HOST";
  private static final Job JOB = Job.newBuilder()
      .setCommand(ImmutableList.of(COMMAND))
      .setImage(IMAGE)
      .setName(JOB_NAME)
      .setVersion("VERSION")
      .build();
  private static final JobId JOB_ID = JOB.getId();

  private ZooKeeperClient client;
  private ZooKeeperMasterModel model;

  private ZooKeeperStandaloneServerManager zk = new ZooKeeperStandaloneServerManager();

  @Before
  public void setup() throws Exception {
    final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(zk.connectString(),
                                                                       retryPolicy);
    curator.start();
    client = new DefaultZooKeeperClient(curator);

    // TODO (dano): this bootstrapping is essentially duplicated from MasterService, should be moved into ZooKeeperMasterModel?
    client.ensurePath(Paths.configHosts());
    client.ensurePath(Paths.configJobs());
    client.ensurePath(Paths.configJobRefs());
    client.ensurePath(Paths.statusHosts());
    client.ensurePath(Paths.statusMasters());
    client.ensurePath(Paths.historyJobs());

    model = new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(client, ZooKeeperModelReporter.noop()));
  }

  @Test
  public void testHostListing() throws Exception {
    final String secondHost = "SECOND";

    assertThat(model.listHosts(), empty());

    model.registerHost(HOST, "foo");
    assertThat(model.listHosts(), contains(HOST));

    model.registerHost(secondHost, "bar");
    assertThat(model.listHosts(), contains(HOST, secondHost));

    model.deregisterHost(HOST);
    assertThat(model.listHosts(), contains(secondHost));
  }

  @Test
  public void testJobCreation() throws Exception {
    assertThat(model.getJobs().entrySet(), empty());
    model.addJob(JOB);

    assertEquals(model.getJobs().get(JOB_ID), JOB);
    assertEquals(model.getJob(JOB_ID), JOB);

    final Job secondJob = Job.newBuilder()
        .setCommand(ImmutableList.of(COMMAND))
        .setImage(IMAGE)
        .setName(JOB_NAME)
        .setVersion("SECOND")
        .build();

    model.addJob(secondJob);
    assertEquals(model.getJob(secondJob.getId()), secondJob);
    assertEquals(2, model.getJobs().size());
  }


  @Test
  public void testJobRemove() throws Exception {
    model.addJob(JOB);
    model.registerHost(HOST, "foo");

    model.deployJob(HOST,
                    Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
    try {
      model.removeJob(JOB_ID);
      fail("should have thrown an exception");
    } catch (JobStillDeployedException e) {
      assertTrue(true);
    }

    model.undeployJob(HOST, JOB_ID);
    assertNotNull(model.getJobs().get(JOB_ID));
    model.removeJob(JOB_ID); // should succeed
    assertNull(model.getJobs().get(JOB_ID));
  }

  @Test
  public void testDeploy() throws Exception {
    try {
      model.deployJob(HOST,
                      Deployment.newBuilder()
                          .setGoal(Goal.START)
                          .setJobId(JOB_ID)
                          .build());
      fail("should throw");
    } catch (JobDoesNotExistException | HostNotFoundException e) {
      assertTrue(true);
    }

    model.addJob(JOB);
    try {
      model.deployJob(HOST,
                      Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (HostNotFoundException e) {
      assertTrue(true);
    }

    model.registerHost(HOST, "foo");

    model.deployJob(HOST,
                    Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());

    model.undeployJob(HOST, JOB_ID);
    model.removeJob(JOB_ID);

    try {
      model.deployJob(HOST,
                      Deployment.newBuilder().setGoal(Goal.START).setJobId(JOB_ID).build());
      fail("should throw");
    } catch (JobDoesNotExistException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testHostRegistration() throws Exception {
    model.registerHost(HOST, "foo");
    List<String> hosts1 = model.listHosts();
    assertThat(hosts1, hasItem(HOST));

    model.deregisterHost(HOST);
    List<String> hosts2 = model.listHosts();
    assertEquals(0, hosts2.size());
  }

  @Test
  public void testUpdateDeploy() throws Exception {
    try {
      stopJob(model, JOB);
      fail("should have thrown JobNotDeployedException");
    } catch (JobNotDeployedException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobNotDeployedException, got " + e.getClass());
    }

    model.addJob(JOB);
    try {
      stopJob(model, JOB);
      fail("should have thrown exception");
    } catch (HostNotFoundException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an HostNotFoundException");
    }

    model.registerHost(HOST, "foo");
    List<String> hosts = model.listHosts();
    assertThat(hosts, hasItem(HOST));

    try {
      stopJob(model, JOB);
      fail("should have thrown exception");
    } catch (JobNotDeployedException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown an JobNotDeployedException");
    }

    model.deployJob(HOST, Deployment.newBuilder()
        .setGoal(Goal.START)
        .setJobId(JOB.getId())
        .build());
    Map<JobId, Job> jobsOnHost = model.getJobs();
    assertEquals(1, jobsOnHost.size());
    Job descriptor = jobsOnHost.get(JOB.getId());
    assertEquals(JOB, descriptor);

    stopJob(model, JOB); // should succeed this time!
    Deployment jobCfg = model.getDeployment(HOST, JOB.getId());
    assertEquals(Goal.STOP, jobCfg.getGoal());
  }

  private void stopJob(ZooKeeperMasterModel model, Job job) throws HeliosException {
    model.updateDeployment(HOST, Deployment.newBuilder()
        .setGoal(Goal.STOP)
        .setJobId(job.getId())
        .build());
  }
}
