/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

public interface NodeUpdaterFactory {

  ZooKeeperNodeUpdater create(String path);
}
