package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

public class Check implements ZooKeeperOperation {

  private static final int ANY_VERSION = -1;

  private final String path;

  public Check(final String path) {
    this.path = path;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.check(path, ANY_VERSION));
  }

  @Override
  public String toString() {
    return "Check{" +
           "path='" + path + '\'' +
           '}';
  }
}
