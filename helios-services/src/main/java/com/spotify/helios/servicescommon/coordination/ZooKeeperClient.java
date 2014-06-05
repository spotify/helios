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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Exists because the Curator library makes things ununit-testable without this. Also it avoids
 * having to catch Exception at call-sites.
 */
public interface ZooKeeperClient {

  void ensurePath(String path) throws KeeperException;

  void ensurePath(String path, boolean excludingLast) throws KeeperException;

  byte[] getData(String path) throws KeeperException;

  List<String> getChildren(String path) throws KeeperException;

  void delete(String path) throws KeeperException;

  void setData(String path, byte[] bytes) throws KeeperException;

  void createAndSetData(String path, byte[] data) throws KeeperException;

  void createWithMode(String path, CreateMode mode) throws KeeperException;

  Stat stat(String path) throws KeeperException;

  void deleteRecursive(String path) throws KeeperException;

  List<String> listRecursive(String path) throws KeeperException;

  void create(String path) throws KeeperException;

  PersistentPathChildrenCache pathChildrenCache(String path, Path snapshotFile) throws IOException;

  Collection<CuratorTransactionResult> transaction(List<ZooKeeperOperation> operations)
      throws KeeperException;

  Collection<CuratorTransactionResult> transaction(ZooKeeperOperation... operations)
      throws KeeperException;

  void delete(String path, int version) throws KeeperException;

  Node getNode(String path) throws KeeperException;

  Stat exists(String path) throws KeeperException;

  Listenable<ConnectionStateListener> getConnectionStateListenable();

  ZooKeeper.States getState() throws KeeperException;

  void start();

  void close();

  PersistentEphemeralNode persistentEphemeralNode(String path,
                                                  final PersistentEphemeralNode.Mode mode,
                                                  byte[] data);

  CuratorFramework getCuratorFramework();
}
