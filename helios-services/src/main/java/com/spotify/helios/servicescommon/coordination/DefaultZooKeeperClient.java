package com.spotify.helios.servicescommon.coordination;

import com.google.common.collect.Lists;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class DefaultZooKeeperClient implements ZooKeeperClient {

  private static final byte[] EMPTY = new byte[0];

  private static final Logger log = LoggerFactory.getLogger(DefaultZooKeeperClient.class);
  public static final int ANY_VERSION = -1;

  private final CuratorFramework client;

  public DefaultZooKeeperClient(CuratorFramework client) {
    this.client = client;
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
    return client;
  }

  @Override
  public void ensurePath(final String path) throws KeeperException {
    ensurePath(path, false);
  }

  @Override
  public void ensurePath(final String path, final boolean excludingLast) throws KeeperException {
    EnsurePath ensurePath = new EnsurePath(path);
    if (excludingLast) {
      ensurePath = ensurePath.excludingLast();
    }
    try {
      ensurePath.ensure(client.getZookeeperClient());
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public byte[] getData(final String path) throws KeeperException {
    try {
      return client.getData().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public Node getNode(final String path) throws KeeperException {
    final Stat stat = new Stat();
    try {
      byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
      return new Node(path, bytes, stat);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public Stat exists(final String path) throws KeeperException {
    try {
      return client.checkExists().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public List<String> getChildren(final String path) throws KeeperException {
    try {
      return client.getChildren().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public List<String> listRecursive(final String path) throws KeeperException {
    try {
      return ZKUtil.listSubTreeBFS(zk(), path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void delete(final String path) throws KeeperException {
    delete(path, ANY_VERSION);
  }

  @Override
  public void delete(final String path, final int version) throws KeeperException {
    try {
      zk().delete(path, version);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void createAndSetData(final String path, final byte[] data) throws KeeperException {
    try {
      zk().create(path, data, OPEN_ACL_UNSAFE, PERSISTENT);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void create(final String path) throws KeeperException {
    createWithMode(path, PERSISTENT);
  }

  @Override
  public void createWithMode(final String path, final CreateMode mode) throws KeeperException {
    try {
      zk().create(path, EMPTY, OPEN_ACL_UNSAFE, mode);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void setData(final String path, final byte[] data) throws KeeperException {
    try {
      zk().setData(path, data, ANY_VERSION);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public Stat stat(final String path) throws KeeperException {
    try {
      return client.checkExists().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public Listenable<ConnectionStateListener> getConnectionStateListenable() {
    return client.getConnectionStateListenable();
  }

  @Override
  public PersistentEphemeralNode persistentEphemeralNode(final String path,
                                                         final PersistentEphemeralNode.Mode mode,
                                                         final byte[] data) {
    return new PersistentEphemeralNode(client, mode, path, data);
  }

  @Override
  public PersistentPathChildrenCache pathChildrenCache(final String path, final Path snapshotFile)
      throws IOException {
    return new PersistentPathChildrenCache(client, path, snapshotFile);
  }

  @Override
  public Collection<OpResult> transaction(final List<ZooKeeperOperation> operations)
      throws KeeperException {

    log.debug("transaction: {}", operations);

    if (operations.isEmpty()) {
      return emptyList();
    }

    // Assemble transaction
    final List<Op> transaction = Lists.newArrayList();
    for (final ZooKeeperOperation operation : operations) {
      try {
        operation.register(transaction);
      } catch (final Exception e) {
        throw propagate(e);
      }
    }

    // Commit
    try {
      return zk().multi(transaction);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public Collection<OpResult> transaction(final ZooKeeperOperation... operations)
      throws KeeperException {
    return transaction(asList(operations));
  }

  private ZooKeeper zk() throws KeeperException {
    try {
      return client.getZookeeperClient().getZooKeeper();
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }
}
