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

import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ZooKeeperClusterIdTest extends SystemTestBase {

  @Test
  public void testClusterId() throws Exception {
    // Create the cluster ID node
    zk().curator().newNamespaceAwareEnsurePath(Paths.configId(zkClusterId))
          .ensure(zk().curator().getZookeeperClient());

    // We need to create a new curator because ZooKeeperClient will try to start it,
    // and zk().curator() has already been started.
    final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.builder()
        .retryPolicy(retryPolicy)
        .connectString(zk().connectString())
        .build();

    ZooKeeperClient client = new DefaultZooKeeperClient(curator, zkClusterId);
    client.start();

    // This should work since the cluster ID exists
    client.create("/test");

    // Now let's remove the cluster ID
    client.delete(Paths.configId(zkClusterId));

    // Sleep so the watcher thread in ZooKeeperClient has a chance to update state
    Thread.sleep(500);

    // Try the same operation again, and it should fail this time
    try {
      client.ensurePath(Paths.configJobs());
      fail("ZooKeeper operation should have failed because cluster ID was removed");
    } catch (IllegalStateException ignore) {
    }
  }

}
