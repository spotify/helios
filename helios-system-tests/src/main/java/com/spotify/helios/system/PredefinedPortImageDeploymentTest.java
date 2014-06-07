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

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PredefinedPortImageDeploymentTest extends SystemTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());

    final HeliosClient client = defaultClient();

    // Create a job using an image exposing port 11211 but without mapping it
    final Job job1 = Job.newBuilder()
        .setName(testTag + "memcached")
        .setVersion("v1")
        .setImage("rohan/memcached-tiny")
        .build();
    final JobId jobId1 = job1.getId();
    client.createJob(job1).get();

    // Create a job using an image exposing port 11211 and map it to a specific external port
    final Job job2 = Job.newBuilder()
        .setName(testTag + "memcached")
        .setVersion("v2")
        .setImage("rohan/memcached-tiny")
        .setPorts(ImmutableMap.of("tcp", PortMapping.of(11211, externalPort)))
        .build();
    final JobId jobId2 = job2.getId();
    client.createJob(job2).get();

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the jobs on the agent
    client.deploy(Deployment.of(jobId1, START), testHost()).get();
    client.deploy(Deployment.of(jobId2, START), testHost()).get();

    // Wait for the jobs to run
    awaitJobState(client, testHost(), jobId1, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    awaitJobState(client, testHost(), jobId2, RUNNING, LONG_WAIT_MINUTES, MINUTES);
  }
}
