package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import com.netflix.curator.framework.api.transaction.CuratorTransactionBridge;

class CreateWithData implements ZooKeeperOperation {

  private final String path;
  private final byte[] data;

  CreateWithData(final String path, final byte[] data) {
    this.path = path;
    this.data = data;
  }

  @Override
  public CuratorTransactionBridge register(final CuratorTransaction transaction) throws Exception {
    return transaction.create().forPath(path, data);
  }
}
