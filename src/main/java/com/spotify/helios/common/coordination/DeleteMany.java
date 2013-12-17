package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;

import java.util.List;

public class DeleteMany implements ZooKeeperOperation {

  private final List<String> paths;

  public DeleteMany(final List<String> paths) {
    this.paths = paths;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    for (final String path : paths) {
      transaction.delete().forPath(path);
    }
  }

  @Override
  public String toString() {
    return "DeleteMany{" +
           "paths=" + paths +
           '}';
  }
}
