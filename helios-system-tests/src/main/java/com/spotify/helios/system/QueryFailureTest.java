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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests that commands which do search matching tell you that you didn't match anything if you
 * provided something to match against.  Specifically, if you do something like
 *     helios -s foo hosts list
 * and no host starts with 'list', it'll at least tell you that no hosts matched that, rather than
 * just returning an empty display
 *
 * @author (Drew Csillag) drewc@spotify.com
 */
public class QueryFailureTest extends SystemTestBase {
  @Test
  public void testHostList() throws Exception {
    startDefaultMaster();

    final String result = cli("hosts", "framazama");
    assertContains("matched no hosts", result);
  }

  @Test
  public void testJobList() throws Exception {
    startDefaultMaster();

    final String result = cli("jobs", "framazama");
    assertContains("matched no jobs", result);
  }

  @Test
  public void testJobStatusJobFilter() throws Exception {
    startDefaultMaster();
    final String result2 = cli("status", "-j", "framazama");
    assertContains("matched no jobs", result2);
  }

  @Test
  public void testJobStatusHostFilter() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();
    startDefaultAgent(getTestHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(JOB_VERSION)
        .setImage("ubuntu:12.04")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(EMPTY_PORTS)
        .build();

    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final String result = cli("status", "--host", "framazama");
    assertContains("matched no hosts", result);
  }
}
