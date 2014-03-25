package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

class Delete implements ZooKeeperOperation {

  private static final int ANY_VERSION = -1;

  private final String path;

  Delete(final String path) {
    this.path = path;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.delete(path, ANY_VERSION));
  }

  @Override
  public String toString() {
    return "Delete{" +
           "path='" + path + '\'' +
           '}';
  }
}
