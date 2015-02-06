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

import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingClusterManager;
import com.spotify.helios.client.HeliosClient;

import org.junit.Test;

public class ZooKeeperBadNodeTest extends SystemTestBase {

  /**
   * This is a testing cluster that has one peer that can't be resolved. Note that this is different
   * from a cluster where a peer is down (that's tested in {@link ZooKeeperHeliosFailoverTest}).
   */
  private final ZooKeeperTestManager zkc = new ZooKeeperTestingClusterManager() {
    @Override
    public String connectString() {
      return super.connectString() + ",node-that-doesnt-exist:1738";
    }
  };

  @Test
  public void testGetJobsWithBadNode() throws Exception {
    startDefaultMaster("--zk-cluster-id=" + zkClusterId);

    final HeliosClient client = defaultClient();
    client.jobs().get();
  }

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return zkc;
  }

}
