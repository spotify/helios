package com.spotify.helios.common;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.utils.EnsurePath;
import com.spotify.helios.common.coordination.ZooKeeperClient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.netflix.curator.framework.imps.CuratorFrameworkState.STARTED;

public class DefaultZooKeeperClient implements ZooKeeperClient {

  private final CuratorFramework client;

  public DefaultZooKeeperClient(CuratorFramework client) {
    checkArgument(client.getState() == STARTED);
    this.client = client;
  }

  @Override
  /** {@inheritDoc} */
  public void ensurePath(final String path) throws KeeperException {
    ensurePath(path, false);
  }

  @Override
  /** {@inheritDoc} */
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
  public List<String> getChildren(final String path) throws KeeperException {
    try {
      return client.getChildren().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void deleteRecursive(final String path) throws KeeperException {
    try {
      ZKUtil.deleteRecursive(client.getZookeeperClient().getZooKeeper(), path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void delete(final String path) throws KeeperException {
    try {
      client.delete().forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void createAndSetData(final String path, final byte[] data) throws KeeperException {
    ensurePath(path, true);
    try {
      client.create().forPath(path, data);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void createWithMode(final String path, final CreateMode mode) throws KeeperException {
    ensurePath(path, true);
    try {
      client.create().withMode(mode).forPath(path);
    } catch (Exception e) {
      propagateIfInstanceOf(e, KeeperException.class);
      throw propagate(e);
    }
  }

  @Override
  public void setData(final String path, final byte[] data) throws KeeperException {
    try {
      client.setData().forPath(path, data);
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
  public PathChildrenCache pathChildrenCache(final String path, final boolean cacheData) {
    return new PathChildrenCache(client, path, cacheData);
  }
}
