/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.coordination;


import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;

public interface NodeUpdaterFactory {

  ZooKeeperNodeUpdater create(String path);
}
