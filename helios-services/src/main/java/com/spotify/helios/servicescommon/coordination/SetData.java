package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

public class SetData implements ZooKeeperOperation {

  private static final int ANY_VERSION = -1;

  private final byte[] bytes;
  private final String path;
  private final int version;

  public SetData(String path, byte[] bytes) {
    this(path, bytes, ANY_VERSION);
  }

  public SetData(String path, byte[] bytes, int version) {
    this.path = path;
    this.bytes = bytes;
    this.version = version;
  }

  @Override
  public void register(final Collection<Op> operations) throws Exception {
    operations.add(Op.setData(path, bytes, version));
  }

  @Override
  public String toString() {
    return "SetData{" +
           ", path='" + path + '\'' +
           ", version=" + version +
           '}';
  }
}
