/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperOperation;

import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperRegistrarServiceUtilTest {

  private static final String HOSTNAME = "host";
  private static final String ID = UUID.randomUUID().toString();
  private static final JobId JOB_ID1 =
      JobId.newBuilder().setName("job1").setVersion("0.1.0").build();
  private static final JobId JOB_ID2 =
      JobId.newBuilder().setName("job2").setVersion("0.2.0").build();
  private static final List<JobId> JOB_IDS = ImmutableList.of(JOB_ID1, JOB_ID2);
  private static final String JOB_STRING1 = JOB_ID1.toString();
  private static final String JOB_STRING2 = JOB_ID2.toString();
  private static final List<String> JOB_STRINGS = ImmutableList.of(JOB_STRING1, JOB_STRING2);

  @Mock ZooKeeperClient zkClient;

  @Test
  public void testRegisterHost() throws Exception {
    final String idPath = Paths.configHostId(HOSTNAME);
    ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, HOSTNAME, ID);
    verify(zkClient).ensurePath(Paths.configHost(HOSTNAME));
    verify(zkClient).ensurePath(Paths.configHostJobs(HOSTNAME));
    verify(zkClient).ensurePath(Paths.configHostPorts(HOSTNAME));
    verify(zkClient).ensurePath(Paths.statusHost(HOSTNAME));
    verify(zkClient).ensurePath(Paths.statusHostJobs(HOSTNAME));
    verify(zkClient).createAndSetData(idPath, ID.getBytes(UTF_8));
  }

  @Test
  public void testDeregisterHost() throws Exception {
    ZooKeeperRegistrarUtil.deregisterHost(zkClient, HOSTNAME);
    verify(zkClient).transaction(anyListOf(ZooKeeperOperation.class));
  }

  // Verify that the re-registering:
  // * does not change the /config/hosts/<host> subtree, except the host-id.
  // * deletes everything under /status/hosts/<host> subtree.
  @Test
  public void testReRegisterHost() throws Exception {
    ZooKeeperTestingServerManager testingServerManager = null;
    try {
      testingServerManager = new ZooKeeperTestingServerManager();
      testingServerManager.awaitUp(5, TimeUnit.SECONDS);

      final ZooKeeperClient zkClient = new DefaultZooKeeperClient(
          testingServerManager.curatorWithSuperAuth());

      // Register the host & add some fake data to its status & config dirs
      final String idPath = Paths.configHostId(HOSTNAME);
      ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, HOSTNAME, ID);
      zkClient.ensurePath(Paths.statusHostJob(HOSTNAME, JOB_ID1));
      zkClient.ensurePath(Paths.configHostJob(HOSTNAME, JOB_ID1));
      final Stat jobConfigStat = zkClient.stat(Paths.configHostJob(HOSTNAME, JOB_ID1));

      // ... and then re-register it
      final String newId = UUID.randomUUID().toString();
      ZooKeeperRegistrarUtil.reRegisterHost(zkClient, HOSTNAME, newId);

      // Verify that the host-id was updated
      assertEquals(newId, new String(zkClient.getData(idPath)));

      // Verify that /status/hosts/<host>/jobs exists and is EMPTY
      assertNotNull(zkClient.exists(Paths.statusHostJobs(HOSTNAME)));
      assertThat(zkClient.listRecursive(Paths.statusHostJobs(HOSTNAME)),
                 contains(Paths.statusHostJobs(HOSTNAME)));
      // Verify that re-registering didn't change the nodes in /config/hosts/<host>/jobs
      assertEquals(
          jobConfigStat,
          zkClient.stat(Paths.configHostJob(HOSTNAME, JOB_ID1))
      );
    } finally {
      if (testingServerManager != null) {
        testingServerManager.close();
      }
    }
  }
}
