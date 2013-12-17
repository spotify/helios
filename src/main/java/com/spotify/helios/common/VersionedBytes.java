package com.spotify.helios.common;

public class VersionedBytes {
  private final byte[] bytes;
  private final int version;

  public VersionedBytes(final byte[] bytes, final int version) {
    this.bytes = bytes;
    this.version = version;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getVersion() {
    return version;
  }
}
