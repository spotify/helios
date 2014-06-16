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

package com.spotify.helios.testing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.system.SystemTestBase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.hasFailureContaining;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class TemporaryJobsTest extends SystemTestBase {

  // Note: This test needs the DOCKER_HOST environment variable set to an address where
  //       containers can be reacher. If docker is running in a local VM, DOCKER_HOST must be set
  //       to the address of that VM.

  private static final Logger log = LoggerFactory.getLogger(TemporaryJobsTest.class);

  // These static fields exist as a way for nested tests to access non-static fields and methods in
  // SystemTestBase. This is a bit ugly, but we can't pass the values to FakeTest, because we don't
  // instantiate it, JUnit does in the PrintableResult.testResult method. And since JUnit
  // instantiates it, it must be a static class, which means it can't access the non-static fields
  // in SystemTestBase.
  private static HeliosClient client;
  private static String testHost;

  @Before
  public void verifyContainerReachability() throws Exception {
    final int dockerProbePort = dockerPortRange().lowerEndpoint();
    final DockerClient docker = new DefaultDockerClient(DOCKER_HOST.uri());
    docker.pull("busybox");
    final ContainerConfig config = ContainerConfig.builder()
        .image("busybox")
        .cmd("nc", "-p", "4711", "-lle", "cat")
        .exposedPorts(ImmutableSet.of("4711/tcp"))
        .build();
    final HostConfig hostConfig = HostConfig.builder()
        .portBindings(ImmutableMap.of("4711/tcp",
                                      asList(PortBinding.of("0.0.0.0", dockerProbePort))))
        .build();
    final ContainerCreation creation = docker.createContainer(config, testTag + "-probe");
    final String containerId = creation.id();
    docker.startContainer(containerId, hostConfig);

    // Wait for container to come up
    Polling.await(5, SECONDS, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final ContainerInfo info = docker.inspectContainer(containerId);
        return info.state().running() ? true : null;
      }
    });

    log.info("Verifying that docker containers are reachable");
    try {
      Polling.awaitUnchecked(5, SECONDS, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          final DefaultProber prober = new DefaultProber();
          log.info("Probing: {}:{}", DOCKER_HOST.address(), dockerProbePort);
          return prober.probe(DOCKER_HOST.address(), dockerProbePort) ? true : null;
        }
      });
    } catch (TimeoutException e) {
      fail("Please ensure that DOCKER_HOST is set to an address that where containers can " +
           "be reached. If docker is running in a local VM, DOCKER_HOST must be set to the " +
           "address of that VM. If docker can only be reached on a limited port range, " +
           "set the environment variable DOCKER_PORT_RANGE=start:end");
    }

    docker.killContainer(containerId);
  }

  private static final class TestProber extends DefaultProber {

    @Override
    public boolean probe(final String host, final int port) {
      // Probe for ports where docker is running instead of on the mock testHost address
      assertEquals(testHost, host);
      return super.probe(DOCKER_HOST.address(), port);
    }
  }

  public static class SimpleTest {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .build();

    private TemporaryJob job1;

    @Before
    public void setup() {
      job1 = temporaryJobs.job()
          .image("busybox")
          .command("nc", "-p", "4711", "-lle", "cat")
          .port("echo", 4711)
          .deploy(testHost);
    }

    @Test
    public void testDeployment() throws Exception {
      // Verify that it is possible to deploy additional jobs during test
      temporaryJobs.job()
          .image("busybox")
          .command(IDLE_COMMAND)
          .host(testHost)
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (Job job : jobs.values()) {
        assertEquals("wrong job running", "busybox", job.getImage());
      }

      //verify address and addresses return valid HostAndPort objects
      assertEquals("wrong host", testHost, job1.address("echo").getHostText());
      assertEquals("wrong host", testHost, getOnlyElement(job1.addresses("echo")).getHostText());

      ping(DOCKER_HOST.address(), job1.port(testHost, "echo"));
    }

    @Test
    public void testRandomHost() throws Exception {
      temporaryJobs.job()
          .image("busybox")
          .command("sh", "-c", "while :; do sleep 5; done")
          .hostFilter(".+")
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 2, jobs.size());
      for (Job job : jobs.values()) {
        assertEquals("wrong job running", "busybox", job.getImage());
      }

      ping(DOCKER_HOST.address(), job1.port(testHost, "echo"));
    }

    public void testDefaultLocalHostFilter() throws Exception {
      temporaryJobs.job()
          .image("busybox")
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    @Test(expected = AssertionError.class)
    public void testExceptionWithBadHostFilter() throws Exception {
      // Shouldn't be able to deploy if filter doesn't match any hosts
      temporaryJobs.job()
          .image("busybox")
          .command("sh", "-c", "while :; do sleep 5; done")
          .hostFilter("THIS_FILTER_SHOULDNT_MATCH_ANY_HOST")
          .deploy();
    }

    @Test
    public void testImageFromBuild() {
      temporaryJobs.job()
          .imageFromBuild()
          .command("sh", "-c", "while :; do sleep 5; done")
          .deploy();
    }

    private void ping(final String host, final int port) throws Exception {
      try (final Socket s = new Socket(host, port)) {
        byte[] ping = "ping".getBytes(UTF_8);
        s.getOutputStream().write(ping);
        final byte[] pong = new byte[4];
        final int n = s.getInputStream().read(pong);
        assertEquals(4, n);
        assertArrayEquals(ping, pong);
      }
    }
  }

  public static class BadTest {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .hostFilter(".*")
        .client(client)
        .prober(new TestProber())
        .build();

    private TemporaryJob job2 = temporaryJobs.job()
        .image("base")
        .deploy(testHost);

    @Test
    public void testFail() throws Exception {
      fail();
    }
  }

  @Test
  public void testRule() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost = testHost();
    startDefaultAgent(testHost);

    awaitHostStatus(client, testHost, UP, LONG_WAIT_MINUTES, MINUTES);

    assertThat(testResult(SimpleTest.class), isSuccessful());
    assertTrue("jobs are running that should not be",
               client.jobs().get(15, SECONDS).isEmpty());
  }

  @Test
  public void verifyJobFailsWhenCalledBeforeTestRun() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost = testHost();
    assertThat(testResult(BadTest.class),
               hasFailureContaining("deploy() must be called in a @Before or in the test method"));
  }
}
