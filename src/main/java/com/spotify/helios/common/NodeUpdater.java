/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import java.io.IOException;

public interface NodeUpdater {

  void update(byte[] bytes) throws IOException;
}
