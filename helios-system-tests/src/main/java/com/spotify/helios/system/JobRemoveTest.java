/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JobRemoveTest extends SystemTestBase {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });
  }

  private Boolean awaitJobUndeployed(final HeliosClient client, final String host,
                                        final JobId jobId,
                                        final int timeout,
                                        final TimeUnit timeunit) throws Exception {
    return Polling.await(timeout, timeunit, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        if (hostStatus == null) {
          return null;
        }
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
        return taskStatus == null ? true : null;
      }
    });
  }

  @Test
  public void testRemoveJobWithoutHistory() throws Exception {
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    final JobDeleteResponse response =
        defaultClient().deleteJob(jobId).get(WAIT_TIMEOUT_SECONDS, SECONDS);
    assertEquals(JobDeleteResponse.Status.OK, response.getStatus());
  }

  @Test
  public void testRemoveJobDeletesHistory() throws Exception {
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    deployJob(jobId, testHost());
    awaitJobState(
        defaultClient(), testHost(), jobId, TaskStatus.State.RUNNING, LONG_WAIT_SECONDS, SECONDS);
    undeployJob(jobId, testHost());
    awaitJobUndeployed(defaultClient(), testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);

    final ZooKeeperClient zkClient = new ZooKeeperClientProvider(
        new DefaultZooKeeperClient(zk().curatorWithSuperAuth()), ZooKeeperModelReporter.noop())
        .get("test-client");

    // Check that there's some history events
    assertNotNull(zkClient.stat(Paths.historyJob(jobId)));

    // Remove job
    final JobDeleteResponse response =
        defaultClient().deleteJob(jobId).get(WAIT_TIMEOUT_SECONDS, SECONDS);
    assertEquals(JobDeleteResponse.Status.OK, response.getStatus());

    // Verify that history is gone
    assertNull(zkClient.stat(Paths.historyJob(jobId)));
  }
}
