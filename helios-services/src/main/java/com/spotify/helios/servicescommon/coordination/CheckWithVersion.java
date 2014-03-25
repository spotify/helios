package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

public class CheckWithVersion implements ZooKeeperOperation {

  private final String path;
  private final int version;

  public CheckWithVersion(final String path, final int version) {
    this.path = path;
    this.version = version;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.check(path, version));
  }

  @Override
  public String toString() {
    return "CheckWithVersion{" +
           "path='" + path + '\'' +
           ", version=" + version +
           '}';
  }
}
