package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import com.netflix.curator.framework.api.transaction.CuratorTransactionBridge;

class CreateEmpty implements ZooKeeperOperation {

  private final String path;

  CreateEmpty(final String path) {
    this.path = path;
  }

  @Override
  public CuratorTransactionBridge register(final CuratorTransaction transaction) throws Exception {
    return transaction.create().forPath(path);
  }
}
