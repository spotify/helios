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

package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A best-effort ZooKeeper node updater.
 */
public class ZooKeeperNodeUpdater implements NodeUpdater {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperNodeUpdater.class);

  private final ZooKeeperClient zooKeeperClient;
  private final String path;

  public ZooKeeperNodeUpdater(final String path, final ZooKeeperClient zooKeeperClient) {
    this.zooKeeperClient = zooKeeperClient;
    this.path = path;
  }

  @Override
  public boolean update(final byte[] bytes) {
    final String parent = ZKPaths.getPathAndNode(path).getPath();
    try {
      if (zooKeeperClient.stat(parent) == null) {
        return false;
      }
      if (zooKeeperClient.stat(path) == null) {
        zooKeeperClient.createAndSetData(path, bytes);
      } else {
        zooKeeperClient.setData(path, bytes);
      }
      return true;
    } catch (KeeperException.NodeExistsException ignore) {
      // Conflict due to curator retry or losing a race. We're done here.
      return true;
    } catch (KeeperException.ConnectionLossException e) {
      log.warn("ZooKeeper connection lost while updating node: {}", path);
      return false;
    } catch (KeeperException e) {
      log.error("failed to update node: {}", path, e);
      return false;
    }
  }
}
