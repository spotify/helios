/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon;


public interface NodeUpdaterFactory {

  ZooKeeperNodeUpdater create(String path);
}
