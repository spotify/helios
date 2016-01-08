/*
 * Copyright (c) 2016 Spotify AB.
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

import com.google.common.collect.Sets;

import com.spotify.helios.servicescommon.coordination.Paths;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.data.ACL;
import org.junit.Test;

import java.util.List;

import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.heliosAclProvider;
import static org.junit.Assert.assertEquals;

public class ZooKeeperAclTest extends SystemTestBase {

  private final ACLProvider aclProvider = heliosAclProvider(MASTER_USER, MASTER_DIGEST,
                                                            AGENT_USER, AGENT_DIGEST);

  /**
   * Verify that the master sets the correct ACLs on the root node on start-up
   */
  @Test
  public void testMasterSetsRootNodeAcls() throws Exception {
    startDefaultMaster();

    final CuratorFramework curator = zk().curatorWithSuperAuth();

    final List<ACL> acls = curator.getACL().forPath("/");
    assertEquals(
        Sets.newHashSet(aclProvider.getAclForPath("/")),
        Sets.newHashSet(acls));
  }

  /**
   * Simple test to make sure nodes created by agents use the ACLs provided by the ACL provider.
   */
  @Test
  public void testAgentCreatedNodesHaveAcls() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);

    final CuratorFramework curator = zk().curatorWithSuperAuth();

    final String path = Paths.statusHostUp(TEST_HOST);
    final List<ACL> acls = curator.getACL().forPath(path);
    assertEquals(
        Sets.newHashSet(aclProvider.getAclForPath(path)),
        Sets.newHashSet(acls));
  }

  /**
   * Simple test to make sure nodes created by master use the ACLs provided by the ACL provider.
   */
  @Test
  public void testMasterCreatedNodesHaveAcls() throws Exception {
    startDefaultMaster();

    final CuratorFramework curator = zk().curatorWithSuperAuth();

    final String path = Paths.statusMasterUp(TEST_MASTER);
    final List<ACL> acls = curator.getACL().forPath(path);
    assertEquals(
        Sets.newHashSet(aclProvider.getAclForPath(path)),
        Sets.newHashSet(acls));
  }
}
