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

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class MultipleHostsTest extends SystemTestBase {
  @Test
  public void testHostStatuses() throws Exception {
    final String aHost = testHost() + "a";
    final String bHost = testHost() + "b";

    startDefaultMaster();
    startDefaultAgent(aHost);
    startDefaultAgent(bHost);
    awaitHostStatus(aHost, UP, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(bHost, UP, LONG_WAIT_SECONDS, SECONDS);

    final Map<String, HostStatus> cliStatuses = new ObjectMapper().readValue(cli("hosts", "--json"),
        new TypeReference<Map<String, HostStatus>>() {});
    assertTrue("status must contain key for " + aHost, cliStatuses.containsKey(aHost));
    assertTrue("status must contain key for " + bHost, cliStatuses.containsKey(bHost));

    final HeliosClient client = defaultClient();
    final Map<String, HostStatus> clientStatuses = client.hostStatuses(
        ImmutableList.of(aHost, bHost)).get();

    assertTrue("status must contain key for " + aHost, clientStatuses.containsKey(aHost));
    assertTrue("status must contain key for " + bHost, clientStatuses.containsKey(bHost));
  }

  @Test
  public void testFilteringJobAndHostStatuses() throws Exception {
    final String aHost = testHost() + "a";
    final String bHost = testHost() + "b";

    startDefaultMaster();
    startDefaultAgent(aHost);
    startDefaultAgent(bHost);
    awaitHostStatus(aHost, UP, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(bHost, UP, LONG_WAIT_SECONDS, SECONDS);

    final HeliosClient client = defaultClient();

    final Job job = Job.newBuilder()
        .setName(testJobName + "I_WANT_THIS_ONE")
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    client.createJob(job).get();

    final Job job2 = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId2 = job2.getId();
    client.createJob(job2).get();

    final Deployment deployment = Deployment.of(jobId, Goal.START);
    client.deploy(deployment, aHost);
    client.deploy(deployment, bHost);
    client.deploy(Deployment.of(jobId2, Goal.START), aHost);

    awaitJobState(client, aHost, jobId, State.RUNNING, LONG_WAIT_SECONDS, TimeUnit.SECONDS);
    awaitJobState(client, bHost, jobId, State.RUNNING, LONG_WAIT_SECONDS, TimeUnit.SECONDS);
    awaitJobState(client, aHost, jobId2, State.RUNNING, LONG_WAIT_SECONDS, TimeUnit.SECONDS);

    final Map<JobId, JobStatus> cliStatuses = new ObjectMapper().readValue(
        cli("status", "--job", "I_WANT_THIS_ONE", "--host", aHost, "--json"),
        new TypeReference<Map<JobId, JobStatus>>() {});
    assertEquals("status should only have one job", 1, cliStatuses.size());
    assertTrue(cliStatuses.containsKey(jobId));
    final JobStatus status = cliStatuses.get(jobId);
    assertEquals("deployments should have only one item", 1, status.getDeployments().size());
    assertTrue("should only have deployment info for aHost",
        status.getDeployments().containsKey(aHost));
    assertEquals("Task statuses should only have one item", 1, status.getTaskStatuses().size());
    assertTrue("should only have status info for aHost",
        status.getTaskStatuses().containsKey(aHost));
  }
}
