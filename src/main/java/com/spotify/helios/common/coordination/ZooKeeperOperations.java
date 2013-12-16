package com.spotify.helios.common.coordination;

import com.spotify.helios.common.descriptors.Descriptor;

public class ZooKeeperOperations {

  public static ZooKeeperOperation create(final String path) {
    return new CreateEmpty(path);
  }

  public static ZooKeeperOperation create(final String path, final Descriptor data) {
    return create(path, data.toJsonBytes());
  }

  public static ZooKeeperOperation create(final String path, final byte[] bytes) {
    return new CreateWithData(path, bytes);
  }

  public static ZooKeeperOperation delete(final String path) {
    return new Delete(path);
  }
}

