package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;

class CreateEmpty implements ZooKeeperOperation {

  private final String path;

  CreateEmpty(final String path) {
    this.path = path;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    transaction.create().forPath(path);
  }
}
