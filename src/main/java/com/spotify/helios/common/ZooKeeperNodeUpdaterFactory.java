/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

public class ZooKeeperNodeUpdaterFactory implements NodeUpdaterFactory {

  final DefaultZooKeeperClient zooKeeperClient;

  public ZooKeeperNodeUpdaterFactory(final DefaultZooKeeperClient zooKeeperClient) {
    this.zooKeeperClient = zooKeeperClient;
  }

  @Override
  public ZooKeeperNodeUpdater create(final String path) {
    return new ZooKeeperNodeUpdater(path, zooKeeperClient);
  }
}
