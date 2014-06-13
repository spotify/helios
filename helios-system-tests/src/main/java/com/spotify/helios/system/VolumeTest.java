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

import com.google.common.io.ByteStreams;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class VolumeTest extends SystemTestBase {

  private static final Logger log = LoggerFactory.getLogger(VolumeTest.class);

  @Test
  public void test() throws Exception {
    startDefaultMaster();

    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .addVolume("/volume")
        .addVolume("/urandom", "/dev/urandom")
        .setCommand(asList("sh", "-c", "echo foo > /volume/bar && " +
                                       "nc -p 4711 -le ls / &&" +
                                       "nc -p 4712 -le ls /dev &&" +
                                       "nc -p 4713 -le dd if=/volume/bar &&" +
                                       "nc -p 4714 -lle dd if=/urandom bs=1 count=4"))
        .addPort("root", PortMapping.of(4711))
        .addPort("dev", PortMapping.of(4712))
        .addPort("bar", PortMapping.of(4713))
        .addPort("urandom", PortMapping.of(4714))
        .build();
    final JobId jobId = job.getId();

    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitHostRegistered(client, testHost(), LONG_WAIT_MINUTES, MINUTES);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, testHost()).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, taskStatus.getJob());

    final Integer root = taskStatus.getPorts().get("root").getExternalPort();
    final Integer dev = taskStatus.getPorts().get("dev").getExternalPort();
    final Integer bar = taskStatus.getPorts().get("bar").getExternalPort();
    final Integer urandom = taskStatus.getPorts().get("urandom").getExternalPort();

    assert root != null;
    assert dev != null;
    assert bar != null;
    assert urandom != null;

    // ls /
    log.info("$ ls /");
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try (final Socket s = new Socket(DOCKER_HOST.address(), root)) {
          final BufferedReader reader = new BufferedReader(
              new InputStreamReader(s.getInputStream()));
          String line;
          while ((line = reader.readLine()) != null) {
            log.info(line);
          }
          return true;
        } catch (IOException e) {
          return null;
        }
      }
    });

    // ls /dev
    log.info("$ ls /dev");
    Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try (final Socket s = new Socket(DOCKER_HOST.address(), dev)) {
          final BufferedReader reader = new BufferedReader(
              new InputStreamReader(s.getInputStream()));
          String line;
          while ((line = reader.readLine()) != null) {
            log.info(line);
          }
          return true;
        } catch (IOException e) {
          return null;
        }
      }
    });

    // Read "foo" from /volume/bar
    final String foo = Polling.await(LONG_WAIT_MINUTES, MINUTES, new Callable<String>() {
      @Override
      public String call() {
        try (final Socket s = new Socket(DOCKER_HOST.address(), bar)) {
          final byte[] foo = new byte[3];
          ByteStreams.readFully(s.getInputStream(), foo);
          return new String(foo, UTF_8);
        } catch (IOException e) {
          return null;
        }
      }
    });
    assertEquals("foo", foo);

    // Attempt to read some random bytes from the mounted /dev/urandom
    try (final Socket s = new Socket(DOCKER_HOST.address(), urandom)) {
      ByteStreams.readFully(s.getInputStream(), new byte[4]);
    }
  }
}
