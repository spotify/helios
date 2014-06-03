package com.spotify.helios.servicescommon;

import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.zookeeper.KeeperException;

public interface ZooKeeperRegistrarEventListener {

  /**
   * Called upon startup
   */
  void startUp() throws Exception;

  /**
   * Called upon shutdown
   */
  void shutDown() throws Exception;

  /**
   * Called when ZK client connects
   *
   * Handler should attempt to do on connection initialization here
   */
  void tryToRegister(final ZooKeeperClient client) throws KeeperException;
}
