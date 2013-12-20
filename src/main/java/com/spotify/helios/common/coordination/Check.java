package com.spotify.helios.common.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransaction;

public class Check implements ZooKeeperOperation {

  private final String path;

  public Check(final String path) {
    this.path = path;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    transaction.check().forPath(path);
  }

  @Override
  public String toString() {
    return "Check{" +
           "path='" + path + '\'' +
           '}';
  }
}
