package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransaction;

import java.util.Map;

public class CreateMany implements ZooKeeperOperation {

  private final Map<String, byte[]> nodes;

  public CreateMany(final Map<String, byte[]> nodes) {
    this.nodes = nodes;
  }

  @Override
  public void register(final CuratorTransaction transaction) throws Exception {
    for (final Map.Entry<String, byte[]> entry : nodes.entrySet()) {
      transaction.create().forPath(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    return "CreateMany{" +
           "nodes=" + nodes.keySet() +
           '}';
  }
}
