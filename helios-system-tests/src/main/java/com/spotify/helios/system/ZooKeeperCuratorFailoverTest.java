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

package com.spotify.helios.system;

import com.spotify.helios.Parallelized;
import com.spotify.helios.ZooKeeperTestingClusterManager;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertArrayEquals;

@Ignore("This is an expensive test that tests the curator framework, not helios. " +
        "It is disabled as it likely gives travis grief")
@RunWith(Parallelized.class)
public class ZooKeeperCuratorFailoverTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  public static final byte[] FOO_DATA = "foo".getBytes();
  public static final String FOO = "/foo";

  private ZooKeeperTestingClusterManager zk;

  @Before
  public void setup() {
    zk = new ZooKeeperTestingClusterManager();
  }

  @After
  public void teardown() {
    zk.close();
  }

  @Test
  public void verifyCanCreateNodesWithOnePeerDead() throws Exception {
    // Kill two peers and bring one up to ensure that only two out of three peers are alive
    zk.stopPeer(0);
    zk.stopPeer(1);
    zk.awaitDown(5, MINUTES);
    zk.startPeer(1);
    zk.awaitUp(5, MINUTES);

    try {
      zk.curatorWithSuperAuth().create().forPath(FOO, FOO_DATA);
      assertArrayEquals(FOO_DATA, zk.curatorWithSuperAuth().getData().forPath(FOO));
    } catch (KeeperException.NodeExistsException ignore) {
    }
  }

  @Test
  public void verifyCanNotCreateNodesWithTwoPeersDead() throws Exception {
    zk.stopPeer(0);
    zk.stopPeer(1);
    zk.awaitDown(5, MINUTES);

    expectedException.expect(KeeperException.ConnectionLossException.class);

    zk.curatorWithSuperAuth().create().forPath(FOO, FOO_DATA);
  }


  @Test
  public void verifyZooKeeperRecoversWithTwoPeersAlive() throws Exception {
    zk.stopPeer(0);
    zk.stopPeer(1);
    zk.awaitDown(5, MINUTES);

    zk.resetPeer(0);
    zk.startPeer(0);
    zk.awaitUp(5, MINUTES);

    try {
      zk.curatorWithSuperAuth().create().forPath(FOO, FOO_DATA);
      assertArrayEquals(FOO_DATA, zk.curatorWithSuperAuth().getData().forPath(FOO));
    } catch (KeeperException.NodeExistsException ignore) {
    }
  }

  @Test
  public void verifyZooKeeperToleratesOneNodeDataLoss() throws Exception {
    try {
      zk.curatorWithSuperAuth().create().forPath(FOO, FOO_DATA);
      assertArrayEquals(FOO_DATA, zk.curatorWithSuperAuth().getData().forPath(FOO));
    } catch (KeeperException.NodeExistsException ignore) {
    }

    zk.stopPeer(0);
    zk.resetPeer(0);
    zk.startPeer(0);

    zk.stopPeer(1);

    assertArrayEquals(FOO_DATA, zk.curatorWithSuperAuth().getData().forPath(FOO));
  }


}
