package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

class CreateEmpty implements ZooKeeperOperation {

  private static final byte[] EMPTY = new byte[0];
  private final String path;

  CreateEmpty(final String path) {
    this.path = path;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.create(path, EMPTY, OPEN_ACL_UNSAFE, PERSISTENT));
  }

  @Override
  public String toString() {
    return "CreateEmpty{" +
           "path='" + path + '\'' +
           '}';
  }
}
