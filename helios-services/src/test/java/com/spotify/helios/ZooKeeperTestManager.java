/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import org.apache.curator.framework.CuratorFramework;

public interface ZooKeeperTestManager {

  /**
   * Start zookeeper.
   */
  void start();

  /**
   * Stop zookeeper.
   */
  void stop();

  /**
   * Ensure a path.
   */
  void ensure(String path) throws Exception;

  /**
   * Tear down zookeeper.
   */
  void close();

  /**
   * Get a connection string for this zk cluster.
   */
  String connectString();

  /**
   * Get a curator client connected to this cluster.
   */
  CuratorFramework curator();
}
