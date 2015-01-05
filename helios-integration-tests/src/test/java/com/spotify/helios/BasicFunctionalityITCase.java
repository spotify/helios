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

import com.google.common.collect.Iterables;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.Polling.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicFunctionalityITCase {

  private static final Logger log = LoggerFactory.getLogger(BasicFunctionalityITCase.class);
  private static final String JOB_NAME = "BasicFunctionalityITCase";

  static final List<String> IDLE_COMMAND = asList(
      "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");
  public static final String BUSYBOX = "busybox";

  private HeliosClient client;
  private String deployHost;
  private JobId id;

  @Before
  public void setUp() throws Exception {
    String endpointUrl = System.getenv("HELIOS_ENDPOINT");
    if (endpointUrl == null) {
      endpointUrl = "http://localhost:5801";
    }
    client = HeliosClient.newBuilder().setUser("unittest").setEndpoints(endpointUrl).build();
  }

  @After
  public void tearDown() throws Exception {
    if (client == null) {
      log.error("Client is null");
      return;
    }
    if (deployHost != null) {
      log.error("undeploy");
      client.undeploy(id, deployHost).get();
    }
    if (id != null) {
      log.error("delete job");
      client.deleteJob(id).get();
    }
  }

  @Test(expected=HeliosException.class)
  public void testComputeTargetSingleEndpointInvalid() throws Exception {
    final HeliosClient badClient =
        HeliosClient.newBuilder().setUser("unittest").setEndpoints("some.fqdn.net").build();
    try {
      badClient.listMasters().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof HeliosException) {
        throw (HeliosException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  @Test
  public void deployJobx() throws Exception {
    final CreateJobResponse createResult = client.createJob(Job.newBuilder()
        .setVersion("" + System.currentTimeMillis())
        .setName(JOB_NAME)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .build()).get();
    id = JobId.fromString(createResult.getId());

    assertEquals(CreateJobResponse.Status.OK, createResult.getStatus());

    final Map<JobId, Job> jobMap = client.jobs(JOB_NAME).get();
    boolean found = false;
    for (Entry<JobId, Job> entry : jobMap.entrySet()) {
      if (entry.getKey().toString().equals(createResult.getId())) {
        found = true;
        break;
      }
    }
    assertTrue("expected to find job I just created in results", found);
  }

  @Test
  public void deployJob() throws Exception {
    final CreateJobResponse createResult = client.createJob(Job.newBuilder()
      .setVersion("" + System.currentTimeMillis())
      .setName(JOB_NAME)
      .setImage(BUSYBOX)
      .setCommand(IDLE_COMMAND)
      .build()).get();
    id = JobId.fromString(createResult.getId());

    assertEquals(CreateJobResponse.Status.OK, createResult.getStatus());

    final List<String> hosts = client.listHosts().get();
    deployHost = Iterables.get(hosts, 0);
    final JobDeployResponse deployResult = client.deploy(
        new Deployment(id, Goal.START, Deployment.EMTPY_DEPLOYER_USER),
        deployHost).get();
    assertEquals(JobDeployResponse.Status.OK, deployResult.getStatus());

    // Wait for it to be running
    final Boolean ok = await(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final JobStatus result = client.jobStatus(id).get();
        final Set<Entry<String, TaskStatus>> statuses = result.getTaskStatuses().entrySet();
        if (statuses.isEmpty()) {
          return null;
        }

        final Entry<String, TaskStatus> last = Iterables.getLast(statuses);
        final TaskStatus status = last.getValue();

        if (!last.getKey().equals(deployHost)) {
          return false;  // something went awry
        }

        if (TaskStatus.State.RUNNING == status.getState()) {
          return true;
        }
        return null;
      }
    });
    assertTrue("deployed to wrong host?!?!", ok);
  }
}
