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

package com.spotify.helios.servicescommon.coordination;

import com.fasterxml.jackson.databind.JavaType;

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

public class ReportingZooKeeperClient implements ZooKeeperClient {

  private final ZooKeeperClient client;
  private final String tag;
  private final ZooKeeperModelReporter reporter;

  public ReportingZooKeeperClient(final ZooKeeperClient client,
                                  final ZooKeeperModelReporter reporter,
                                  final String tag) {
    this.client = client;
    this.tag = tag;
    this.reporter = reporter;
  }

  @Override
  public void ensurePath(String path) throws KeeperException {
    try {
      client.ensurePath(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "ensurePath");
      throw e;
    }
  }

  @Override
  public void ensurePath(String path, boolean excludingLast) throws KeeperException {
    try {
      client.ensurePath(path, excludingLast);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "ensurePath");
      throw e;
    }
  }

  @Override
  public byte[] getData(String path) throws KeeperException {
    try {
      return client.getData(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "getData");
      throw e;
    }
  }

  @Override
  public List<String> getChildren(String path) throws KeeperException {
    try {
      return client.getChildren(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "getChildren");
      throw e;
    }
  }

  @Override
  public void delete(String path) throws KeeperException {
    try {
      client.delete(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "delete");
      throw e;
    }
  }

  @Override
  public void setData(String path, byte[] bytes) throws KeeperException {
    try {
      client.setData(path, bytes);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "setData");
      throw e;
    }
  }

  @Override
  public void createAndSetData(String path, byte[] data) throws KeeperException {
    try {
      client.createAndSetData(path, data);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "createAndSetData");
      throw e;
    }
  }

  @Override
  public void createWithMode(String path, CreateMode mode) throws KeeperException {
    try {
      client.createWithMode(path, mode);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "createWithMode");
      throw e;
    }
  }

  @Override
  public Stat stat(String path) throws KeeperException {
    try {
      return client.stat(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "stat");
      throw e;
    }
  }

  @Override
  public void deleteRecursive(String path) throws KeeperException {
    try {
      client.deleteRecursive(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "deleteRecursive");
      throw e;
    }
  }

  @Override
  public List<String> listRecursive(String path) throws KeeperException {
    try {
      return client.listRecursive(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "listRecursive");
      throw e;
    }
  }

  @Override
  public void create(String path) throws KeeperException {
    try {
      client.create(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "create");
      throw e;
    }
  }

  @Override
  public <T> PersistentPathChildrenCache<T> pathChildrenCache(String path, Path snapshotFile,
                                                              final JavaType valueType)
      throws IOException, InterruptedException {
    return client.pathChildrenCache(path, snapshotFile, valueType);
  }

  @Override
  public Collection<CuratorTransactionResult> transaction(List<ZooKeeperOperation> operations)
      throws KeeperException {
    try {
      return client.transaction(operations);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "transaction");
      throw e;
    }
  }

  @Override
  public Collection<CuratorTransactionResult> transaction(ZooKeeperOperation... operations)
      throws KeeperException {
    try {
      return client.transaction(operations);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "transaction");
      throw e;
    }
  }

  @Override
  public void delete(String path, int version) throws KeeperException {
    try {
      client.delete(path, version);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "delete");
      throw e;
    }
  }

  @Override
  public Node getNode(String path) throws KeeperException {
    try {
      return client.getNode(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "getNode");
      throw e;
    }
  }

  @Override
  public Stat exists(String path) throws KeeperException {
    try {
      return client.exists(path);
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "exists");
      throw e;
    }
  }

  @Override
  public Listenable<ConnectionStateListener> getConnectionStateListenable() {
    return client.getConnectionStateListenable();
  }

  @Override
  public ZooKeeper.States getState() throws KeeperException {
    try {
      return client.getState();
    } catch (KeeperException e) {
      reporter.checkException(e, tag, "getState");
      throw e;
    }
  }

  @Override
  public PersistentEphemeralNode persistentEphemeralNode(final String path,
                                                         final PersistentEphemeralNode.Mode mode,
                                                         final byte[] data) {
    return client.persistentEphemeralNode(path, mode, data);
  }

  @Override
  public void start() {
    client.start();
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public CuratorFramework getCuratorFramework() {
    return client.getCuratorFramework();
  }
}

