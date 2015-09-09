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

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.authentication.HeliosAuthException;
import com.spotify.helios.cli.TestUtils;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DockerVersion;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HostListCommandTest {

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private HostListCommand command;

  private static final List<String> HOSTS = ImmutableList.of("host2.", "host1.", "host3.");

  private static final String JOB_NAME = "job";
  private static final String JOB_VERSION1 = "1-aaa";
  private static final String JOB_VERSION2 = "3-ccc";
  private static final String JOB_VERSION3 = "2-bbb";

  private static final JobId JOB_ID1 = new JobId(JOB_NAME, JOB_VERSION1);
  private static final JobId JOB_ID2 = new JobId(JOB_NAME, JOB_VERSION2);
  private static final JobId JOB_ID3 = new JobId(JOB_NAME, JOB_VERSION3);

  private static final Job JOB1 = Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION1).build();
  private static final Job JOB2 = Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION2).build();
  private static final Job JOB3 = Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION3).build();

  private static final Map<JobId, Deployment> JOBS = ImmutableMap.of(
      JOB_ID1, Deployment.newBuilder().build(),
      JOB_ID2, Deployment.newBuilder().build(),
      JOB_ID3, Deployment.newBuilder().build()
  );

  private static final Map<JobId, TaskStatus> JOB_STATUSES = ImmutableMap.of(
      JOB_ID1,TaskStatus.newBuilder().setJob(JOB1).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      JOB_ID2, TaskStatus.newBuilder().setJob(JOB2).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      JOB_ID3, TaskStatus.newBuilder().setJob(JOB3).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build()
  );

  private static final Map<String, String> LABELS = ImmutableMap.of("foo", "bar", "baz", "qux");

  private static final List<String> EXPECTED_ORDER = ImmutableList.of("host1.", "host2.", "host3.");


  @Before
  public void setUp() throws ParseException, HeliosAuthException {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("hosts");
    command = new HostListCommand(subparser);

    when(options.getString("pattern")).thenReturn("");
    when(client.listHosts()).thenReturn(Futures.immediateFuture(HOSTS));

    final HostInfo hostInfo = HostInfo.newBuilder()
        .setCpus(4)
        .setMemoryTotalBytes((long) Math.pow(1024, 3))
        .setMemoryFreeBytes(500000000)
        .setLoadAvg(0.1)
        .setOsName("OS foo")
        .setOsVersion("0.1.0")
        .setDockerVersion(DockerVersion.builder().version("1.7.0").apiVersion("1.18").build())
        .build();

    final long dayMilliseconds = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    final long startTime = System.currentTimeMillis() - 2 * dayMilliseconds;

    final AgentInfo agentInfo = AgentInfo.newBuilder()
        .setVersion("0.8.420")
        .setUptime(dayMilliseconds)
        .setStartTime(startTime)
        .build();

    final HostStatus status = HostStatus.newBuilder()
        .setJobs(JOBS)
        .setStatuses(JOB_STATUSES)
        .setStatus(UP)
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setLabels(LABELS)
        .build();

    final HostStatus downStatus = HostStatus.newBuilder()
        .setJobs(JOBS)
        .setStatuses(JOB_STATUSES)
        .setStatus(DOWN)
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setLabels(LABELS)
        .build();

    final Map<String, HostStatus> statuses = ImmutableMap.of(
        HOSTS.get(0), status,
        HOSTS.get(1), status,
        HOSTS.get(2), downStatus
    );

    when(client.hostStatuses(anyListOf(String.class), anyMapOf(String.class, String.class)))
        .thenReturn(Futures.immediateFuture(statuses));
  }

  @Test
  public void testCommand() throws Exception {
    final int ret = command.run(options, client, out, false, null);
    final String output = baos.toString();

    assertEquals(0, ret);
    assertThat(output, containsString(
        "HOST      STATUS        DEPLOYED    RUNNING    CPUS    MEM     LOAD AVG    MEM USAGE    "
        + "OS              HELIOS     DOCKER          LABELS"));
    assertThat(output, containsString(
        "host1.    Up 2 days     3           3          4       1 gb    0.10        0.53         "
        + "OS foo 0.1.0    0.8.420    1.7.0 (1.18)    foo=bar, baz=qux"));
    assertThat(output, containsString(
        "host2.    Up 2 days     3           3          4       1 gb    0.10        0.53         "
        + "OS foo 0.1.0    0.8.420    1.7.0 (1.18)    foo=bar, baz=qux"));
    assertThat(output, containsString(
        "host3.    Down 1 day    3           3          4       1 gb    0.10        0.53         "
        + "OS foo 0.1.0    0.8.420    1.7.0 (1.18)    foo=bar, baz=qux"));
  }

  @Test
  public void testQuietOutputIsSorted() throws Exception {
    when(options.getBoolean("q")).thenReturn(true);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    assertEquals(EXPECTED_ORDER, TestUtils.readFirstColumnFromOutput(baos.toString(), false));
  }

  @Test
  public void testNonQuietOutputIsSorted() throws Exception {
    when(options.getBoolean("q")).thenReturn(false);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    assertEquals(EXPECTED_ORDER, TestUtils.readFirstColumnFromOutput(baos.toString(), true));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidStatusThrowsError() throws Exception {
    when(options.getString("status")).thenReturn("DWN");
    final int ret = command.run(options, client, out, false, null);
    final String output = baos.toString();

    assertEquals(1, ret);
    assertThat(output, equalToIgnoringWhiteSpace("Invalid status. Valid statuses are: UP, DOWN"));
  }
}