package com.spotify.helios.common.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransaction;

public class SetData implements ZooKeeperOperation {

  private final byte[] bytes;
  private final String path;

  public SetData(String path, byte[] bytes) {
    this.path = path;
    this.bytes = bytes;
  }

  @Override
  public void register(CuratorTransaction transaction) throws Exception {
    transaction.setData().forPath(path, bytes);
  }

}
