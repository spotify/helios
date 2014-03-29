/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.coordination;


public class ZooKeeperNodeUpdaterFactory implements NodeUpdaterFactory {

  final ZooKeeperClient zooKeeperClient;

  public ZooKeeperNodeUpdaterFactory(final ZooKeeperClient zooKeeperClient) {
    this.zooKeeperClient = zooKeeperClient;
  }

  @Override
  public ZooKeeperNodeUpdater create(final String path) {
    return new ZooKeeperNodeUpdater(path, zooKeeperClient);
  }
}
