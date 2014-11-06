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
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.STOP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class PortCollisionJobTest extends SystemTestBase {

  private final int externalPort = temporaryPorts.localPort("external");

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final Job job1 = Job.newBuilder()
        .setName(testTag + "foo")
        .setVersion("1")
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10001, externalPort)))
        .build();

    final Job job2 = Job.newBuilder()
        .setName(testTag + "bar")
        .setVersion("1")
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10002, externalPort)))
        .build();

    final CreateJobResponse created1 = client.createJob(job1).get();
    assertEquals(CreateJobResponse.Status.OK, created1.getStatus());

    final CreateJobResponse created2 = client.createJob(job2).get();
    assertEquals(CreateJobResponse.Status.OK, created2.getStatus());

    final Deployment deployment1 = Deployment.of(job1.getId(), STOP);
    final JobDeployResponse deployed1 = client.deploy(deployment1, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed1.getStatus());

    final Deployment deployment2 = Deployment.of(job2.getId(), STOP);
    final JobDeployResponse deployed2 = client.deploy(deployment2, testHost()).get();
    assertEquals(JobDeployResponse.Status.PORT_CONFLICT, deployed2.getStatus());
  }
}
