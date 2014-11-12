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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobListCommandTest {

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private JobListCommand command;

  private final List<String> expectedOrder = ImmutableList.of(
      "job:1-aaa", "job:2-bbb", "job:3-ccc");

  private final Map<JobId, Job> jobs = ImmutableMap.of(
      new JobId("job", "1-aaa"), Job.newBuilder().build(),
      new JobId("job", "3-ccc"), Job.newBuilder().build(),
      new JobId("job", "2-bbb"), Job.newBuilder().build());

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("list");
    command = new JobListCommand(subparser);

    when(client.jobs()).thenReturn(Futures.immediateFuture(jobs));

    Map<JobId, JobStatus> statuses = new HashMap<>();
    for (final JobId jobId : jobs.keySet()) {
      // pretend each job is deployed
      final JobStatus status = JobStatus.newBuilder()
          .setDeployments(ImmutableMap.of("host", Deployment.of(jobId, Goal.START)))
          .build();
      statuses.put(jobId, status);
    }

    when(client.jobStatuses(jobs.keySet())).thenReturn(Futures.immediateFuture(statuses));
  }

  @Test
  public void testQuietOutputIsSorted() throws Exception {
    when(options.getBoolean("q")).thenReturn(true);

    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);

    assertEquals(expectedOrder, readJobIdsFromOutput(false));
  }

  @Test
  public void testNonQuietOutputIsSorted() throws Exception {
    when(options.getBoolean("q")).thenReturn(false);

    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);

    assertEquals(expectedOrder, readJobIdsFromOutput(true));
  }

  /**
   * Parse the output from the command to determine the jobIds that were output, in order. This is
   * a little dicey since it needs to know the format of what the command is outputting to the
   * PrintStream.
   */
  private List<String> readJobIdsFromOutput(final boolean skipFirstRow) {
    final String output = baos.toString();
    final List<String> jobIds = new ArrayList<>();
    for (final String line : output.split("\n")) {
      String jobId = line.split(" ")[0];
      jobIds.add(jobId);
    }
    if (skipFirstRow) {
      return jobIds.subList(1, jobIds.size());
    }
    return jobIds;
  }

}