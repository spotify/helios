/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios;

import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

  /**
   * Await zookeeper successfully serving requests.
   */
  void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException;

  /**
   * Await zookeeper not able to serve requests.
   */
  void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException;
}
