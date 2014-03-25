package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

class CreateWithData implements ZooKeeperOperation {

  private final String path;
  private final byte[] data;

  CreateWithData(final String path, final byte[] data) {
    this.path = path;
    this.data = data;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.create(path, data, OPEN_ACL_UNSAFE, PERSISTENT));
  }

  @Override
  public String toString() {
    return "CreateWithData{" +
           "path='" + path + '\'' +
           '}';
  }
}
