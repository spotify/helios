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

package com.spotify.helios.servicescommon.coordination;

import com.fasterxml.jackson.databind.JavaType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * This class instruments ZooKeeper calls by timing them and reporting exceptions.
 * Calls are delegated to a {@link ZooKeeperClient}.
 */
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
    reporter.time(tag, "ensurePath", () -> {
      client.ensurePath(path);
      return null;
    });
  }

  @Override
  public void ensurePath(String path, boolean excludingLast) throws KeeperException {
    reporter.time(tag, "ensurePath", () -> {
      client.ensurePath(path, excludingLast);
      return null;
    });
  }

  @Override
  public void ensurePathAndSetData(String path, byte[] data) throws KeeperException {
    reporter.time(tag, "ensurePathAndSetData", () -> {
      client.ensurePathAndSetData(path, data);
      return null;
    });
  }

  @Override
  public byte[] getData(String path) throws KeeperException {
    return reporter.time(tag, "getData", () -> client.getData(path));
  }

  @Override
  public List<String> getChildren(String path) throws KeeperException {
    return reporter.time(tag, "getChildren", () -> client.getChildren(path));
  }

  @Override
  public void delete(String path) throws KeeperException {
    reporter.time(tag, "delete", () -> {
      client.delete(path);
      return null;
    });
  }

  @Override
  public void delete(String path, int version) throws KeeperException {
    reporter.time(tag, "delete", () -> {
      client.delete(path, version);
      return null;
    });
  }

  @Override
  public void setData(String path, byte[] bytes) throws KeeperException {
    reporter.time(tag, "setData", () -> {
      client.setData(path, bytes);
      return null;
    });
  }

  @Override
  public void createAndSetData(String path, byte[] data) throws KeeperException {
    reporter.time(tag, "createAndSetData", () -> {
      client.createAndSetData(path, data);
      return null;
    });
  }

  @Override
  public void createWithMode(String path, CreateMode mode) throws KeeperException {
    reporter.time(tag, "createWithMode", () -> {
      client.createWithMode(path, mode);
      return null;
    });
  }

  @Override
  public Stat stat(String path) throws KeeperException {
    return reporter.time(tag, "stat", () -> client.stat(path));
  }

  @Override
  public void deleteRecursive(String path) throws KeeperException {
    reporter.time(tag, "deleteRecursive", () -> {
      client.deleteRecursive(path);
      return null;
    });
  }

  @Override
  public List<String> listRecursive(String path) throws KeeperException {
    return reporter.time(tag, "listRecursive", () -> client.listRecursive(path));
  }

  @Override
  public void create(String path) throws KeeperException {
    reporter.time(tag, "create", () -> {
      client.create(path);
      return null;
    });
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
    return reporter.time(tag, "transaction", () -> client.transaction(operations));
  }

  @Override
  public Collection<CuratorTransactionResult> transaction(ZooKeeperOperation... operations)
      throws KeeperException {
    return reporter.time(tag, "transaction", () -> client.transaction(operations));
  }

  @Override
  public Node getNode(String path) throws KeeperException {
    return reporter.time(tag, "getNode", () -> client.getNode(path));
  }

  @Override
  public Stat exists(String path) throws KeeperException {
    return reporter.time(tag, "exists", () -> client.exists(path));
  }

  @Override
  public Listenable<ConnectionStateListener> getConnectionStateListenable() {
    return client.getConnectionStateListenable();
  }

  @Override
  public ZooKeeper.States getState() throws KeeperException {
    return reporter.time(tag, "getState", client::getState);
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
    client.getConnectionStateListenable().addListener(
        (client, newState) -> reporter.connectionStateChanged(newState));
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public CuratorFramework getCuratorFramework() {
    return client.getCuratorFramework();
  }

  @Override
  public void setAcl(final String path, final List<ACL> aclList) throws KeeperException {
    reporter.time(tag, "setAcl", () -> {
      client.setAcl(path, aclList);
      return null;
    });
  }

  @Override
  public List<ACL> getAcl(final String path) throws KeeperException {
    return reporter.time(tag, "getAcl", () -> client.getAcl(path));
  }
}
