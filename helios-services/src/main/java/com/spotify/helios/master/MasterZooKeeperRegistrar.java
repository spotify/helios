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

package com.spotify.helios.master;

import com.spotify.helios.servicescommon.ZooKeeperRegistrarEventListener;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;

public class MasterZooKeeperRegistrar implements ZooKeeperRegistrarEventListener {

  private static final Logger log = LoggerFactory.getLogger(MasterZooKeeperRegistrar.class);

  private final String name;
  private PersistentEphemeralNode upNode;

  public MasterZooKeeperRegistrar(String name) {
    this.name = name;
  }

  @Override
  public void startUp() throws Exception {

  }

  @Override
  public void shutDown() throws Exception {
    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node: {}", e.getMessage());
      }
    }
  }

  @Override
  public void tryToRegister(final ZooKeeperClient client) throws KeeperException {

    client.ensurePath(Paths.configHosts());
    client.ensurePath(Paths.configJobs());
    client.ensurePath(Paths.configJobRefs());
    client.ensurePath(Paths.statusHosts());
    client.ensurePath(Paths.statusMasters());
    client.ensurePath(Paths.historyJobs());

    if (upNode == null) {
      final String upPath = Paths.statusMasterUp(name);
      upNode = client.persistentEphemeralNode(upPath, Mode.EPHEMERAL, new byte[]{});
      upNode.start();
    }

    log.info("ZooKeeper registration complete");
  }
}
