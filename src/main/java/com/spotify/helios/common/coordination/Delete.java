package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;

class Delete implements ZooKeeperOperation {

  private final String path;

  Delete(final String path) {
    this.path = path;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    transaction.delete().forPath(path);
  }
}
