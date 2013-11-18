/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

public class ZooKeeperNodeUpdaterFactory implements NodeUpdaterFactory {

  final ZooKeeperCurator zooKeeperClient;

  public ZooKeeperNodeUpdaterFactory(final ZooKeeperCurator zooKeeperClient) {
    this.zooKeeperClient = zooKeeperClient;
  }

  @Override
  public ZooKeeperNodeUpdater create(final String path) {
    return new ZooKeeperNodeUpdater(path, zooKeeperClient);
  }
}
