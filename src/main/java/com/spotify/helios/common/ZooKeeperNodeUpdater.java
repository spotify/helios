/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A best-effort ZooKeeper node updater.
 */
public class ZooKeeperNodeUpdater implements NodeUpdater {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperNodeUpdater.class);

  private final DefaultZooKeeperClient zooKeeperClient;
  private final String path;

  public ZooKeeperNodeUpdater(final String path, final DefaultZooKeeperClient zooKeeperClient) {
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
    } catch (KeeperException.ConnectionLossException e) {
      log.warn("ZooKeeper connection lost while updating node: {}", path);
      return false;
    } catch (KeeperException e) {
      log.error("failed to update node: {}", path, e);
      return false;
    }
  }
}
