package com.spotify.helios.common.coordination;

import com.spotify.helios.common.descriptors.Descriptor;

import java.util.List;
import java.util.Map;

public class ZooKeeperOperations {

  public static ZooKeeperOperation check(final String path, final int version) {
    return new CheckWithVersion(path, version);
  }

  public static ZooKeeperOperation check(final String path) {
    return new Check(path);
  }

  public static ZooKeeperOperation create(final Map<String, byte[]> nodes) {
    return new CreateMany(nodes);
  }

  public static ZooKeeperOperation create(final String path) {
    return new CreateEmpty(path);
  }

  public static ZooKeeperOperation create(final String path, final Descriptor data) {
    return create(path, data.toJsonBytes());
  }

  public static ZooKeeperOperation create(final String path, final Descriptor data,
                                          final int version) {
    return create(path, data.toJsonBytes(), version);
  }

  public static ZooKeeperOperation create(final String path, final byte[] bytes,
                                          final int version) {
    return new CreateWithDataAndVersion(path, bytes, version);
  }

  public static ZooKeeperOperation create(final String path, final byte[] bytes) {
    return new CreateWithData(path, bytes);
  }

  public static ZooKeeperOperation delete(final String path) {
    return new Delete(path);
  }

  public static ZooKeeperOperation delete(final List<String> paths) {
    return new DeleteMany(paths);
  }
}

