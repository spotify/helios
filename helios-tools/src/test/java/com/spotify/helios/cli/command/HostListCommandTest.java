/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli.command;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
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
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.junit.Before;
import org.junit.Test;

public class HostListCommandTest {

  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private static final String JOB_NAME = "job";
  private static final String JOB_VERSION1 = "1-aaa";
  private static final String JOB_VERSION2 = "3-ccc";
  private static final String JOB_VERSION3 = "2-bbb";

  private static final JobId JOB_ID1 = new JobId(JOB_NAME, JOB_VERSION1);
  private static final JobId JOB_ID2 = new JobId(JOB_NAME, JOB_VERSION2);
  private static final JobId JOB_ID3 = new JobId(JOB_NAME, JOB_VERSION3);

  private static final Job JOB1 =
      Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION1).build();
  private static final Job JOB2 =
      Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION2).build();
  private static final Job JOB3 =
      Job.newBuilder().setName(JOB_NAME).setVersion(JOB_VERSION3).build();

  private static final Map<JobId, Deployment> JOBS = ImmutableMap.of(
      JOB_ID1, Deployment.newBuilder().build(),
      JOB_ID2, Deployment.newBuilder().build(),
      JOB_ID3, Deployment.newBuilder().build()
  );

  private static final Map<JobId, TaskStatus> JOB_STATUSES = ImmutableMap.of(
      JOB_ID1, TaskStatus.newBuilder().setJob(JOB1).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      JOB_ID2, TaskStatus.newBuilder().setJob(JOB2).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build(),
      JOB_ID3, TaskStatus.newBuilder().setJob(JOB3).setGoal(Goal.START)
          .setState(TaskStatus.State.RUNNING).build()
  );

  private static final Map<String, String> LABELS = ImmutableMap.of("foo", "bar", "baz", "qux");

  private static final List<String> EXPECTED_ORDER = ImmutableList.of("host1.", "host2.", "host3.");
  private HostStatus upStatus;
  private HostStatus downStatus;

  @Before
  public void setUp() throws ParseException {
    // purposefully in non-sorted order so that tests that verify that the output is sorted are
    // actually testing HostListCommand behavior and not accidentally testing what value the
    // mock returns
    final List<String> hosts = ImmutableList.of("host3.", "host1.", "host2.");
    when(client.listHosts()).thenReturn(immediateFuture(hosts));

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

    upStatus = HostStatus.newBuilder()
        .setJobs(JOBS)
        .setStatuses(JOB_STATUSES)
        .setStatus(UP)
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setLabels(LABELS)
        .build();

    downStatus = HostStatus.newBuilder()
        .setJobs(JOBS)
        .setStatuses(JOB_STATUSES)
        .setStatus(DOWN)
        .setHostInfo(hostInfo)
        .setAgentInfo(agentInfo)
        .setLabels(LABELS)
        .build();

    final Map<String, HostStatus> statuses = ImmutableMap.of(
        "host3.", downStatus,
        "host1.", upStatus,
        "host2.", upStatus
    );

    when(client.hostStatuses(eq(hosts), anyMapOf(String.class, String.class)))
        .thenReturn(immediateFuture(statuses));
  }

  @Test
  public void testCommand() throws Exception {
    final int ret = runCommand();
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

  private int runCommand(String... commandArgs)
      throws ExecutionException, InterruptedException, ArgumentParserException {

    final String[] args = new String[1 + commandArgs.length];
    args[0] = "hosts";
    System.arraycopy(commandArgs, 0, args, 1, commandArgs.length);

    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("hosts");
    final HostListCommand command = new HostListCommand(subparser);

    final Namespace options = parser.parseArgs(args);
    return command.run(options, client, out, false, null);
  }

  @Test
  public void testQuietOutputIsSorted() throws Exception {
    final int ret = runCommand("-q");

    assertEquals(0, ret);
    assertEquals(EXPECTED_ORDER, TestUtils.readFirstColumnFromOutput(baos.toString(), false));
  }

  @Test
  public void testNonQuietOutputIsSorted() throws Exception {
    final int ret = runCommand();

    assertEquals(0, ret);
    assertEquals(EXPECTED_ORDER, TestUtils.readFirstColumnFromOutput(baos.toString(), true));
  }

  @Test(expected = ArgumentParserException.class)
  public void testInvalidStatusThrowsError() throws Exception {
    runCommand("--status", "DWN");
  }

  @Test
  public void testPatternFilter() throws Exception {
    final String hostname = "host1.example.com";
    final List<String> hosts = ImmutableList.of(hostname);
    when(client.listHosts("host1")).thenReturn(Futures.immediateFuture(hosts));

    final Map<String, HostStatus> statusResponse = ImmutableMap.of(hostname, upStatus);

    when(client.hostStatuses(eq(hosts), anyMapOf(String.class, String.class)))
        .thenReturn(Futures.immediateFuture(statusResponse));

    final int ret = runCommand("host1");
    assertEquals(0, ret);

    assertEquals(ImmutableList.of("HOST", hostname + "."),
        TestUtils.readFirstColumnFromOutput(baos.toString(), false));
  }

  @Test
  public void testSelectorFilter() throws Exception {
    final String hostname = "foo1.example.com";
    final List<String> hosts = ImmutableList.of(hostname);
    when(client.listHosts(ImmutableSet.of("foo=bar"))).thenReturn(Futures.immediateFuture(hosts));

    final Map<String, HostStatus> statusResponse = ImmutableMap.of(hostname, upStatus);

    when(client.hostStatuses(eq(hosts), anyMapOf(String.class, String.class)))
        .thenReturn(Futures.immediateFuture(statusResponse));

    final int ret = runCommand("--selector", "foo=bar");
    assertEquals(0, ret);

    assertEquals(ImmutableList.of("HOST", hostname + "."),
        TestUtils.readFirstColumnFromOutput(baos.toString(), false));
  }

  /**
   * Verify that the configuration of the '--selector' argument does not cause positional arguments
   * specified after the optional argument to be greedily parsed. This verifies a fix for a bug
   * where `nargs("+")` was used where it was not necessary, which causes a command like `--selector
   * foo=bar pattern` to be interpreted as two selectors of `foo=bar` and `pattern`.
   */
  @Test
  public void testSelectorSlurping() throws Exception {
    final List<String> hosts = ImmutableList.of("host-1");

    when(client.hostStatuses(eq(hosts), anyMapOf(String.class, String.class)))
        .thenReturn(Futures.immediateFuture(Collections.<String, HostStatus>emptyMap()));

    when(client.listHosts("blah", ImmutableSet.of("foo=bar")))
        .thenReturn(Futures.immediateFuture(hosts));

    assertThat(runCommand("-s", "foo=bar", "blah"), equalTo(0));
    assertThat(runCommand("blah", "-s", "foo=bar"), equalTo(0));

    when(client.listHosts("blarp", ImmutableSet.of("a=b", "z=1")))
        .thenReturn(Futures.immediateFuture(hosts));

    assertThat(runCommand("-s", "a=b", "-s", "z=1", "blarp"), equalTo(0));
    assertThat(runCommand("blarp", "--selector", "a=b", "--selector", "z=1"), equalTo(0));
  }
}
