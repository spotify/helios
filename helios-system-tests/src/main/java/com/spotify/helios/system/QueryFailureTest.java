/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;
import org.junit.Test;

/**
 * Tests that commands which do search matching tell you that you didn't match anything if you
 * provided something to match against.  Specifically, if you do something like
 * helios -s foo hosts list
 * and no host starts with 'list', it'll at least tell you that no hosts matched that, rather than
 * just returning an empty display, unless option -q (quiet) is used.
 *
 * @author (Drew Csillag) drewc@spotify.com
 */
public class QueryFailureTest extends SystemTestBase {
  @Test
  public void testHostList() throws Exception {
    startDefaultMaster();

    final String result = cli("hosts", "framazama");
    assertThat(result, containsString("matched no hosts"));
  }

  @Test
  public void testQuietHostList() throws Exception {
    startDefaultMaster();

    final String result = cli("hosts", "framazama", "-q");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJobList() throws Exception {
    startDefaultMaster();

    final String result = cli("jobs", "framazama");
    assertThat(result, containsString("matched no jobs"));
  }

  @Test
  public void testQuietJobList() throws Exception {
    startDefaultMaster();

    final String result = cli("jobs", "framazama", "-q");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testJobStatusJobFilter() throws Exception {
    startDefaultMaster();
    final String result2 = cli("status", "-j", "framazama");
    assertThat(result2, containsString("matched no jobs"));
  }

  @Test
  public void testJobStatusHostFilter() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();
    startDefaultAgent(testHost());

    // Create a job
    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setPorts(EMPTY_PORTS)
        .build();

    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final String result = cli("status", "--host", "framazama");
    assertThat(result, containsString("There are no jobs deployed to hosts with the host pattern"));
  }
}
