package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.zookeeper.KeeperException;

/**
 * Created by snc on 4/22/14.
 */
public interface ZooKeeperClientConnectListener {

  /**
   * Called when ZK client connects
   *
   * Handler should attempt to do on connection initialization here
   *
   * @throws KeeperException
   */
  void onConnect(final SettableFuture<Void> complete) throws KeeperException;

}
