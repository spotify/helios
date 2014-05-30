package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.SettableFuture;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;

/**
 * Created by snc on 4/22/14.
 */
public interface ZooKeeperRegistrarEventListener {

  /**
   * Called upon startup
   *
   */
  void startUp() throws Exception;

  /**
   * Called upon shutdown
   *
   */
  void shutDown() throws Exception;

  /**
   * Called when ZK client connects
   *
   * Handler should attempt to do on connection initialization here
   *
   * @throws KeeperException
   */
  void tryToRegister(final ZooKeeperClient client) throws KeeperException;
}
