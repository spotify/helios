package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;

public class CheckWithVersion implements ZooKeeperOperation {

  private final String path;
  private final int version;

  public CheckWithVersion(final String path, final int version) {
    this.path = path;
    this.version = version;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    transaction.check().withVersion(version).forPath(path);
  }

  @Override
  public String toString() {
    return "CheckWithVersion{" +
           "path='" + path + '\'' +
           ", version=" + version +
           '}';
  }
}
