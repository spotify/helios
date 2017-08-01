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

import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import java.util.Map;
import org.junit.Test;

public class MultipleJobsTest extends SystemTestBase {
  @Test
  public void jobStatusBulk() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostRegistered(testHost(), LONG_WAIT_SECONDS, SECONDS);

    final HeliosClient client = defaultClient();

    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId jobId = job.getId();
    client.createJob(job).get();

    final Job job2 = Job.newBuilder()
        .setName(testJobName + "2")
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();
    final JobId job2Id = job2.getId();
    client.createJob(job2).get();

    final Deployment deployment = Deployment.of(jobId, START, TEST_USER);
    final Deployment deployment2 = Deployment.of(job2Id, START, TEST_USER);

    client.deploy(deployment, testHost()).get();
    awaitJobState(client, testHost(), jobId, RUNNING,
        LONG_WAIT_SECONDS, SECONDS);

    client.deploy(deployment2, testHost()).get();
    awaitJobState(client, testHost(), job2Id, RUNNING,
        LONG_WAIT_SECONDS, SECONDS);

    final Map<JobId, JobStatus> statuses = client.jobStatuses(ImmutableSet.of(jobId, job2Id)).get();
    assertTrue("should contain job 1 id", statuses.containsKey(jobId));
    assertTrue("should contain job 2 id", statuses.containsKey(job2Id));
  }
}
