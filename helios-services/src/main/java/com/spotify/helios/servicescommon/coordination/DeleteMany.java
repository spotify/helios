package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;
import java.util.List;

public class DeleteMany implements ZooKeeperOperation {

  private static final int ANY_VERSION = -1;

  private final List<String> paths;

  public DeleteMany(final List<String> paths) {
    this.paths = paths;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    for (final String path : paths) {
      operations.add(Op.delete(path, ANY_VERSION));
    }
  }

  @Override
  public String toString() {
    return "DeleteMany{" +
           "paths=" + paths +
           '}';
  }
}
