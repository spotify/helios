package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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

  Collection<CuratorTransactionResult> transaction(List<ZooKeeperOperation> operations) throws KeeperException;

  Collection<CuratorTransactionResult> transaction(ZooKeeperOperation... operations) throws KeeperException;

  void delete(String path, int version) throws KeeperException;

  Node getNode(String path) throws KeeperException;

  Stat exists(String path) throws KeeperException;

  void start();

  void close();
}
