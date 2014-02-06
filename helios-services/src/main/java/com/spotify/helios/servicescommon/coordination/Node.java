package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.data.Stat;

public class Node {

  private final String path;
  private final byte[] bytes;
  private final Stat stat;

  public Node(final String path, final byte[] bytes, final Stat stat) {
    this.path = path;
    this.bytes = bytes;
    this.stat = stat;
  }

  public String getPath() {
    return path;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public Stat getStat() {
    return stat;
  }
}
