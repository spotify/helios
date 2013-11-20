/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

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
  public void update(final byte[] bytes) {
    // TODO (dano): implement retries and make it asynchronous in order to enable fire-and-forget use
    try {
      if (zooKeeperClient.stat(path) == null) {
        zooKeeperClient.createAndSetData(path, bytes);
      } else {
        zooKeeperClient.setData(path, bytes);
      }
    } catch (KeeperException e) {
      log.error("failed to update node: {}", path, e);
    }
  }
}
