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

import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.digest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.master.MasterZooKeeperRegistrar;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.util.List;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperAclInitializerTest {

  private static final String AGENT_USER = "agent-user";
  private static final String AGENT_PASSWORD = "agent-pass";

  private static final String MASTER_USER = "master-user";
  private static final String MASTER_PASSWORD = "master-pass";
  private static final List<AuthInfo> MASTER_AUTH = Lists.newArrayList(new AuthInfo(
      "digest", String.format("%s:%s", MASTER_USER, MASTER_PASSWORD).getBytes()));

  private static final String CLUSTER_ID = "helios";

  private ZooKeeperTestManager zk;

  private ACLProvider aclProvider;

  @Before
  public void setUp() {
    aclProvider = ZooKeeperAclProviders.heliosAclProvider(
        MASTER_USER, digest(MASTER_USER, MASTER_PASSWORD),
        AGENT_USER, digest(AGENT_USER, AGENT_PASSWORD));
    zk = new ZooKeeperTestingServerManager();
  }

  @After
  public void tearDown() throws Exception {
    if (zk != null) {
      zk.stop();
    }
  }

  @Test
  public void testInitializeAcl() throws Exception {
    // setup the initial helios tree
    final ZooKeeperClient zkClient = new DefaultZooKeeperClient(zk.curatorWithSuperAuth());
    zkClient.ensurePath(Paths.configId(CLUSTER_ID));
    new MasterZooKeeperRegistrar("helios-master").tryToRegister(zkClient);

    // to start with, nothing should have permissions
    for (final String path : zkClient.listRecursive("/")) {
      assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, zkClient.getAcl(path));
    }

    // initialize ACL's
    ZooKeeperAclInitializer.initializeAcl(zk.connectString(), CLUSTER_ID,
        MASTER_USER, MASTER_PASSWORD,
        AGENT_USER, AGENT_PASSWORD);

    for (final String path : zkClient.listRecursive("/")) {
      final List<ACL> expected = aclProvider.getAclForPath(path);
      final List<ACL> actual = zkClient.getAcl(path);

      assertEquals(expected.size(), actual.size());
      assertTrue(expected.containsAll(actual));
    }
  }
}
