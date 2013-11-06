/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.coordination;

import org.apache.zookeeper.common.PathUtils;

public class PathFactory {

  private final String base;

  public PathFactory(final String base, final String... parts) {
    final String joined = join(base, parts);
    this.base = joined.startsWith("/") ? joined : "/" + joined;
    PathUtils.validatePath(base);
  }

  public String path(final String... parts) {
    final String path = join(base, parts);
    PathUtils.validatePath(path);
    return path;
  }

  private String join(final String base, final String... parts) {
    final StringBuilder builder = new StringBuilder(base);
    for (final String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      if (builder.charAt(builder.length() - 1) != '/' && part.charAt(0) != '/') {
        builder.append('/');
      }
      builder.append(part);
    }
    return builder.toString();
  }
}
