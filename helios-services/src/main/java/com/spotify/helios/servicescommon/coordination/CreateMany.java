package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;
import java.util.Map;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class CreateMany implements ZooKeeperOperation {

  private final Map<String, byte[]> nodes;

  public CreateMany(final Map<String, byte[]> nodes) {
    this.nodes = nodes;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    for (final Map.Entry<String, byte[]> entry : nodes.entrySet()) {
      operations.add(Op.create(entry.getKey(), entry.getValue(), OPEN_ACL_UNSAFE, PERSISTENT));
    }
  }

  @Override
  public String toString() {
    return "CreateMany{" +
           "nodes=" + nodes.keySet() +
           '}';
  }
}
