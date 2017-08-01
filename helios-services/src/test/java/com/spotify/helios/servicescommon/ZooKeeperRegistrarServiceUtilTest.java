/*-
 * -\-\-
 * Helios Services
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

package com.spotify.helios.servicescommon;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperRegistrarServiceUtilTest {

  private static final String HOSTNAME = "host";
  private static final String ID = UUID.randomUUID().toString();
  private static final JobId JOB_ID1 =
      JobId.newBuilder().setName("job1").setVersion("0.1.0").build();

  private ZooKeeperTestingServerManager testingServerManager;
  private ZooKeeperClient zkClient;

  @Before
  public void setUp() throws Exception {
    testingServerManager = new ZooKeeperTestingServerManager();
    testingServerManager.awaitUp(5, TimeUnit.SECONDS);
    zkClient = new DefaultZooKeeperClient(testingServerManager.curatorWithSuperAuth());
  }

  @After
  public void tearDown() throws Exception {
    zkClient.close();
    if (testingServerManager != null) {
      testingServerManager.close();
    }
  }

  @Test
  public void testRegisterHost() throws Exception {
    final String idPath = Paths.configHostId(HOSTNAME);
    ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, HOSTNAME, ID);

    assertNotNull(zkClient.exists(Paths.configHost(HOSTNAME)));
    assertNotNull(zkClient.exists(Paths.configHostJobs(HOSTNAME)));
    assertNotNull(zkClient.exists(Paths.configHostPorts(HOSTNAME)));
    assertNotNull(zkClient.exists(Paths.statusHost(HOSTNAME)));
    assertNotNull(zkClient.exists(Paths.statusHostJobs(HOSTNAME)));
    assertEquals(ID, new String(zkClient.getData(idPath)));
  }

  @Test
  public void testDeregisterHost() throws Exception {
    final String idPath = Paths.configHostId(HOSTNAME);
    ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, HOSTNAME, ID);

    ZooKeeperRegistrarUtil.deregisterHost(zkClient, HOSTNAME);

    assertNull(zkClient.exists(Paths.configHost(HOSTNAME)));
    assertNull(zkClient.exists(Paths.statusHost(HOSTNAME)));
  }

  // Verify that the re-registering:
  // * does not change the /config/hosts/<host> subtree, except the host-id.
  // * deletes everything under /status/hosts/<host> subtree.
  @Test
  public void testReRegisterHost() throws Exception {
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
  }
}
